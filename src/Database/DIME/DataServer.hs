{-# LANGUAGE ScopedTypeVariables, BangPatterns, ViewPatterns, RecordWildCards, NamedFieldPuns #-}
module Database.DIME.DataServer
    (
     dataServerMain --, dataServerSimpleClient
    ) where
import qualified  Control.Exception as E
import Control.Applicative
import Control.Exception.Base
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Seq

import qualified Data.ByteString as SB
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Set as S
import Data.Binary
import Data.String
import Data.List
import Data.Function
import Data.Typeable
import Data.IORef

import qualified Database.DIME.DataServer.State as ServerState
import qualified Database.DIME.Server.Peers as Peers
import qualified Database.DIME.Memory.BlockInfo as BI
import Database.DIME
import Database.DIME.DataServer.Config
import Database.DIME.DataServer.Command
import Database.DIME.DataServer.Response
import Database.DIME.Util
import Database.DIME.Transport
import Database.DIME.Memory.Block
import Database.DIME.Flow hiding (BlockInfo)
import Database.DIME.Flow.TimeSeries (isTimeSeries, asTimeSeries, readTimeSeries, forceTimeSeries)
import Database.DIME.Server.State (PeerName(..))

import GHC.Conc (numCapabilities)

import Language.Flow.Compile
import Language.Flow.Execution.Types
import Language.Flow.Execution.GMachine

import qualified Network.Socket as Net

import qualified System.ZMQ3 as ZMQ
import System.Log.Logger
import System.Console.Haskeline hiding (throwTo)
import System.Console.Haskeline.IO
import System.IO
import System.Time
import System.IO.Unsafe
import System.Posix.Signals
import System.Exit

-- For module use only
moduleName = "Database.DIME.DataServer"

data SigTermReceived = SigTermReceived
                     deriving (Show, Typeable)

instance Exception SigTermReceived

keepAliveTime = 10000000
dumpDataPeriod = 5000000 -- one minute

{-# NOINLINE termSignalReceived #-}
termSignalReceived :: IORef Bool
termSignalReceived = unsafePerformIO $ newIORef False

{-# NOINLINE threadsRef #-}
threadsRef :: TVar (S.Set ThreadId)
threadsRef = unsafePerformIO $ newTVarIO S.empty

runWithTermHandler :: IO () -> IO ()
runWithTermHandler action =
    E.catch (forever action) (\(e :: SigTermReceived) -> return ())

untilTerm :: IO () -> IO ()
untilTerm action = do
  runWithTermHandler action
  status <- readIORef termSignalReceived
  if status then
      do
        return ()
   else untilTerm action

termHandler :: IO ()
termHandler = do
  threads <- atomically $ readTVar threadsRef
  forM (S.toList threads) (\threadId -> throwTo threadId SigTermReceived)
  writeIORef termSignalReceived True

dataServerMain :: String -> String -> IO ()
dataServerMain coordinatorName localAddress = do
  let coordinatorServerName = ipv4 (fromIntegral coordinatorPort) coordinatorName
      coordinatorQueryDealerName = ipv4 (fromIntegral queryBrokerPort) coordinatorName
  infoM moduleName "DIME Data server starting up..."
  initFlowEngine
  mainThreadId <- myThreadId
  atomically $ modifyTVar threadsRef (S.insert mainThreadId)
  installHandler softwareTermination (CatchOnce termHandler) Nothing
  installHandler keyboardSignal (CatchOnce termHandler) Nothing
  stateVar <- ServerState.mkEmptyServerState "dime-data"
  infoM moduleName "DIME data loaded..."
  withContext $ \c -> do
      infoM moduleName "Waiting for request broker..."

      forkIO (requestHandler c stateVar coordinatorServerName)
      forkIO (queryHandler c stateVar coordinatorServerName)

      untilTerm $ saveDataPeriodically stateVar
      infoM moduleName "Going to dump data..."
      ServerState.dumpState stateVar
      exitWith ExitSuccess
      return ()
  where
    statusClient stateVar coordinator hostName ctxt =
        withAttachedSocket ctxt (Connect coordinator) $
          \s -> do
            let zmqName = PeerName $ ipv4 (fromIntegral dataPort) hostName
            untilTerm $ do
              blockCount <- withMVar stateVar (return . ServerState.getBlockCount)
              sendOneRequest s (Peers.UpdateInfo zmqName blockCount) $
                (\response ->
                  case response of
                    Peers.InfoRequest -> return () -- should do something here
                    _ -> return ())
              threadDelay keepAliveTime

    requestHandler c stateVar coordinatorServerName =
      withAttachedSocket c (bindingPort (fromIntegral dataPort)) $ \s -> do
        forkIO $ statusClient stateVar coordinatorServerName localAddress c
        forever $ do
          (s', _) <- accept s
          launchMainLoop c stateVar coordinatorServerName s'
    queryHandler c stateVar coordinatorServerName =
      withAttachedSocket c (bindingPort (fromIntegral queryBrokerPort)) $ \s -> forever $ do
        (s', _) <- accept s
        launchQueryLoop c coordinatorServerName s'

    saveDataPeriodically stateVar = do
      threadDelay dumpDataPeriod
      ServerState.dumpState stateVar

    launchMainLoop c stateVar coordinatorServerName s = do
      loopId <- forkIO $ mainLoop c stateVar coordinatorServerName s
      atomically $ modifyTVar threadsRef (S.insert loopId)

    launchQueryLoop c coordinatorServerName s = do
      loopId <- forkIO $ queryLoop c coordinatorServerName s
      atomically $ modifyTVar threadsRef (S.insert loopId)

    queryLoop c coordinatorName s = do
      threadId <- myThreadId
      runWithTermHandler $
       serveRequest s (return ()) $
       \(cmd :: Command) -> do
         case cmd of
           RunQuery key txt -> doRunQuery c coordinatorName key txt
           _ -> return undefined
      atomically $ modifyTVar threadsRef (S.delete threadId)

    mainLoop c stateVar coordinatorServerName s = do
      threadId <- myThreadId
      runWithTermHandler $ loop s c coordinatorServerName stateVar -- Run loop once (it creates more listeners as it moves)
      atomically $ modifyTVar threadsRef (S.delete threadId)

    expandRowIds regions = concatMap (\(x, y) -> [x..(y - BlockRowID 1)]) regions

    loop s c coordinatorName stateVar = do
      putStrLn "Looping!"
      serveRequest s (return ()) $
         \(cmd :: Command) -> do
            infoM moduleName $ "Got command: " ++ show cmd
            case cmd of
              RunQuery key txt -> doRunQuery c coordinatorName key txt -- this runs in the IO monad, non-atomically
              _ -> modifyMVar stateVar $ \state ->do
                (response, !newState) <- case cmd of -- Parse the command
                  FetchRows tableId rowIds columnIds ->
                    doFetchRows tableId (expandRowIds rowIds) columnIds state
                  UpdateRows tableId rowIds columnIds values ->
                    doUpdateRows tableId rowIds columnIds values state
                  -- Block commands
                  NewBlock blockSpec startRowId endRowId columnType ->
                    doNewBlock blockSpec startRowId endRowId columnType state
                  DeleteBlock blockSpec ->
                    doDeleteBlock blockSpec state
                  BlockInfo blockSpec ->
                    doBlockInfo blockSpec state
                  Map op inputs output ->
                    doMap op inputs output state
                  collapseOp@Collapse {} ->
                    doCollapse collapseOp state
                  ForceComputation blockSpec ->
                    doForceComputation blockSpec state
                return (maybe state id newState, response)

    defaultProgramCellCount = 4096

    doRunQuery c coordinatorName key txt =
      E.catch (do
              let queryState = QueryState { queryKey = key,
                                            queryServerName = coordinatorName,
                                            queryZMQContext = c}
              (state, ret) <- runProgramFromText defaultProgramCellCount queryState txt
              let readEntireGraph = do
                    tosAddr <- topOfStack
                    allPoints <- findReachablePoints [tosAddr]
                    graphData <- mapM (\addr -> do
                                         val <- readGraph addr
                                         return (addr, val)) allPoints
                    return (tosAddr, graphData)
              Right (_, (retAddr, graph)) <- runGMachine readEntireGraph state
              let Just retVal = lookup retAddr graph
              case retVal of
                _
                 | isInteger retVal -> return $ QueryResponse $ IntResult $ asInteger retVal
                 | isString retVal -> return $ QueryResponse $ StringResult $ asString retVal
                 | isDouble retVal -> return $ QueryResponse $ DoubleResult $ asDouble retVal
                 | isTimeSeries retVal -> do
                            Right (_, tsData) <- let retValD = asTimeSeries retVal
                                                 in runGMachine (forceTimeSeries retValD >>
                                                                 readTimeSeries retValD) state
                            return $ QueryResponse $ TimeSeriesResult tsData
                 | otherwise -> return $ Fail "Invalid type returned from query")
            (\(e :: SomeException) -> do
               errorM moduleName $ "Exception during Query: " ++ show e
               return $ Fail $ show e)

    doFetchRows tableId rowIds columnIds state = do
      let -- Make sure supplied data are valid
          validTable = ServerState.hasTable tableId state
          validColumns = all (\c -> ServerState.hasColumn tableId c state) columnIds
      rowsExist <- concat <$> mapM (\c -> mapM (\r -> ServerState.hasRow tableId c r state) rowIds) columnIds
      if validTable && validColumns && all id rowsExist
        then do
          let fetchRow rowId = mapM (fetchColumn rowId) columnIds
              fetchColumn rowId columnId = ServerState.fetchColumnForRow tableId columnId rowId state
          values <- mapM fetchRow rowIds
          return (FetchRowsResponse values, Nothing)
        else return (InconsistentArguments, Nothing)

    doUpdateRows tableId rowIds columnIds values state = do -- Basic idea here is to update each column one at a time
      let values' = map snd $ sortBy (compare `on` fst) $ zip rowIds values -- Reorder the values so that they correspond to rowIds in ascending order (makes it easier to group them)
          rowIds' = sort rowIds

          columnValues = transpose values' -- Put values from a list of row values into a list of column values
          columnTypes = map ((withColumnValue typeOf).head) columnValues

          idsAndValues = zip columnIds columnValues

            -- Check if the values provided are all of the same type
          typeCheckColumn typeRep values = all (\x -> typeRep == withColumnValue typeOf x) values
          valuesAreSane = all id $ map (uncurry typeCheckColumn) $ zip columnTypes columnValues

          updateRows (columnId, columnValues) st = ServerState.updateRows tableId columnId rowIds' columnValues st
      if valuesAreSane
        then do
          state' <- foldl (>>=) (return state) (map updateRows idsAndValues)
          return (Ok, Just state')
        else return (InconsistentTypes, Nothing)

    doNewBlock blockSpec@(BlockSpec tableId columnId blockId) startRowId endRowId columnType state =
      if ServerState.hasBlock blockSpec state
      then return (BlockAlreadyExists, Nothing)
      else do
        let a .- b = (fromIntegral a) - (fromIntegral b)
        newBlock <- ServerState.emptyBlockFromType startRowId (1 + endRowId .- startRowId) columnType
        newState <- ServerState.insertBlock blockSpec newBlock state >>=
                    ServerState.establishRowMapping (tableId, columnId) (startRowId, endRowId) blockId
        return (Ok, Just newState)

    doBlockInfo blockSpec state =
        if ServerState.hasBlock blockSpec state
          then do
            blockInfo <- ServerState.getBlockInfo blockSpec state
            return (BlockInfoResponse blockInfo, Just state)
          else
            return (BlockDoesNotExist, Nothing)

    doDeleteBlock blockSpec state =
      if ServerState.hasBlock blockSpec state
        then do
          ServerState.deleteBlock blockSpec state
          return (Ok, Nothing)
        else return (BlockDoesNotExist, Nothing)

    doMap op inputs output state = do
      let allInputsExist = all (flip ServerState.hasBlock state) inputs
      if allInputsExist
        then do
             mapResult <- ServerState.mapServerBlock op inputs output state
             case mapResult of
               Left _ -> return (InconsistentTypes, Nothing) -- TODO use error parameter
               Right state' -> do
                 blockInfo <- ServerState.getBlockInfo output state'
                 return (MapResponse (BI.blockType blockInfo), Just state')
        else return (BlockDoesNotExist, Nothing)

    doCollapse collapseOp@Collapse {collapseBlockSpec} state =
      if ServerState.hasBlock collapseBlockSpec state
        then do
          collapseResult <- ServerState.collapseServerBlock collapseOp state
          case collapseResult of
            Left _ -> return (InconsistentTypes, Nothing) -- TODO use error parameter
            Right state' -> return (Ok, Just state')
        else return (BlockDoesNotExist, Nothing)

    doForceComputation blockSpec state =
      if ServerState.hasBlock blockSpec state
        then do
          ServerState.forceCompute blockSpec state
          return (Ok, Nothing)
        else return (BlockDoesNotExist, Nothing)

{-| Implements a simple client to the server above.
    Commands are entered directly using Haskell read syntax. Responses are parsed
    and printed using Show
-}
-- dataServerSimpleClient :: IO ()
-- dataServerSimpleClient = do
--   hd <- initializeInput defaultSettings -- Initialize haskeline
--   infoM moduleName "Welcome to DIME data server debug client..."
--   withContext $ \c ->
--      withAttachedSocket c (Connect (ipv4 8008 "127.0.0.1")) $
--           \s -> forever $ loop s hd
--     where
--       loop s hd = do
--         line <- queryInput hd (getInputLine "% ")
--         case line of
--           Just lineData -> do
--               result <- E.catch (E.evaluate $ Just $ read lineData) (\(e :: E.SomeException) -> return Nothing)
--               case result of
--                 Nothing -> putStrLn $ "Could not parse request"
--                 Just (cmd :: Command) -> do
--                              putStrLn $ "Sending command"
--                              startTime <- getClockTime
--                              sendOneRequest s cmd (putStrLn . show :: Response -> IO ())
--                              endTime <- getClockTime
--                              let timeTaken = endTime `diffClockTimes` startTime
--                              putStrLn $ "Query took " ++ (show $ tdMin timeTaken) ++ " m, " ++ (show $ tdSec timeTaken) ++ "s, " ++ (show $ (tdPicosec timeTaken) `div` 1000000000) ++ "ms."
