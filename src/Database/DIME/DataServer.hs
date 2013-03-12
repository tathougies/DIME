{-# LANGUAGE ScopedTypeVariables, BangPatterns, ViewPatterns #-}
module Database.DIME.DataServer
    (
     dataServerMain, dataServerSimpleClient
    ) where
import qualified  Control.Exception as E
import Control.Exception.Base
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.SampleVar
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
    E.catch action (\(e :: SigTermReceived) -> return ())

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
  let coordinatorServerName = "tcp://" ++ coordinatorName ++ ":" ++ show coordinatorPort
      coordinatorQueryDealerName = "tcp://" ++ coordinatorName ++ ":" ++ show queryBrokerPort
  infoM moduleName "DIME Data server starting up..."
  initFlowEngine
  mainThreadId <- myThreadId
  atomically $ modifyTVar threadsRef (S.insert mainThreadId)
  installHandler softwareTermination (CatchOnce termHandler) Nothing
  installHandler keyboardSignal (CatchOnce termHandler) Nothing
  stateVar <- ServerState.mkEmptyServerState "dime-data"
  infoM moduleName "DIME data loaded..."
  withContext $ \c -> do
      requestSyncVar <- (newEmptyMVar :: IO (MVar ()))
      querySyncVar <- (newEmptyMVar :: IO (MVar ()))
      statusId <- forkIO $ statusClient stateVar coordinatorServerName localAddress c
      distributorId <- forkIO $ requestDistributor c requestSyncVar
      queryBrokerId <- forkIO $ queryBroker c coordinatorQueryDealerName querySyncVar
      atomically $ modifyTVar threadsRef (S.insert statusId . S.insert distributorId)

      infoM moduleName "Waiting for request broker..."

      takeMVar requestSyncVar
      takeMVar querySyncVar

      replicateM 3 $ launchMainLoop c stateVar coordinatorServerName
      replicateM 3 $ launchQueryLoop c coordinatorServerName

      untilTerm $ saveDataPeriodically stateVar
      infoM moduleName "Going to dump data..."
      dumpData stateVar
      exitWith ExitSuccess
      return ()
  where
    statusClient stateVar coordinatorName hostName ctxt =
        safelyWithSocket ctxt Req (Connect coordinatorName) $
          \s -> do
            let zmqName = PeerName $ "tcp://" ++ hostName ++ ":" ++ show dataPort
            untilTerm $ do
                  state <- readTVarIO stateVar
                  sendOneRequest s (Peers.UpdateInfo zmqName $ ServerState.getBlockCount state) $
                        (\response ->
                             case response of
                               Peers.InfoRequest -> return () -- should do something here
                               _ -> return ())
                  threadDelay keepAliveTime

    dumpData stateVar = ServerState.dumpState stateVar

    saveDataPeriodically stateVar = do
      threadDelay dumpDataPeriod
      dumpData stateVar

    queryBroker c queryDealerName syncVar =
      safelyWithSocket c Router (Connect queryDealerName) $ \routerS ->
          safelyWithSocket c Dealer (Bind "inproc://queries") $ \dealerS -> do
              putMVar syncVar ()
              forkIO $ untilTerm $ tunnel routerS dealerS
              untilTerm $ tunnel dealerS routerS

    requestDistributor c syncVar =
      safelyWithSocket c Router (Bind $ "tcp://127.0.0.1:" ++ show dataPort) $ \routerS ->
          safelyWithSocket c Dealer (Bind "inproc://requests") $ \dealerS -> do
              putMVar syncVar ()
              forkIO $ untilTerm $ tunnel routerS dealerS
              untilTerm $ tunnel dealerS routerS

    launchMainLoop c stateVar coordinatorServerName = do
      loopId <- forkIO $ mainLoop c stateVar coordinatorServerName
      atomically $ modifyTVar threadsRef (S.insert loopId)

    launchQueryLoop c coordinatorServerName = do
      loopId <- forkIO $ queryLoop c coordinatorServerName
      atomically $ modifyTVar threadsRef (S.insert loopId)

    queryLoop c coordinatorName = runWithTermHandler $ do
        safelyWithSocket c Rep (Connect "inproc://queries") $
               \s -> serveRequest s (return ()) $
                     \(cmd :: Command) -> do
                       launchQueryLoop c coordinatorName
                       case cmd of
                         RunQuery key txt -> doRunQuery c coordinatorName key txt
                         _ -> return undefined

    mainLoop c stateVar coordinatorServerName = runWithTermHandler $ do
        threadId <- myThreadId
        safelyWithSocket c Rep (Connect "inproc://requests") $
               \s -> loop s c coordinatorServerName stateVar -- Run loop once (it creates more listeners as it moves)
        atomically $ modifyTVar threadsRef (S.delete threadId)

    expandRowIds regions = concatMap (\(x, y) -> [x..(y - RowID 1)]) regions

    loop s c coordinatorName stateVar = do
      serveRequest s (return ()) $
         \(cmd :: Command) -> do
            launchMainLoop c stateVar coordinatorName
            case cmd of
              RunQuery key txt -> doRunQuery c coordinatorName key txt -- this runs in the IO monad, non-atomically
              _ -> do
                (response, state') <- atomically $ do
                    state <- readTVar stateVar
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
                                ForceComputation blockSpec ->
                                    doForceComputation blockSpec state
                    case newState of
                      Just state' -> do
                          writeTVar stateVar state'
                          return (response, state')
                      Nothing -> return (response, state)
                E.evaluate (state' `using` rseq)
                return response

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

    doFetchRows tableId rowIds columnIds state =
        let values = map fetchRow rowIds
            fetchRow rowId = map (fetchColumn rowId) columnIds
            fetchColumn rowId columnId = ServerState.fetchColumnForRow tableId columnId rowId state

            -- Make sure supplied data are valid
            validTable = ServerState.hasTable tableId state
            validColumns = all (\c -> ServerState.hasColumn tableId c state) columnIds
            validRows = all (\c -> all (\r -> ServerState.hasRow tableId c r state) rowIds) columnIds

            -- Make sure tables exist first, then columns, then rows
            validTablesColumnsAndRows = validTable && validColumns && validRows
        in -- Make sure all rows and columns exist
          if validTablesColumnsAndRows
          then return (FetchRowsResponse values, Just state)
          else return (InconsistentArguments, Nothing)

    doUpdateRows tableId rowIds columnIds values state = -- Basic idea here is to update each column one at a time
        let values' = map snd $ sortBy (compare `on` fst) $ zip rowIds values -- Reorder the values so that they correspond to rowIds in ascending order (makes it easier to group them)
            rowIds' = sort rowIds

            columnValues = transpose values' -- Put values from a list of row values into a list of column values
            columnTypes = map ((withColumnValue typeOf).head) columnValues

            idsAndValues = zip columnIds columnValues
            updateRows (columnId, columnValues) st = ServerState.updateRows tableId columnId rowIds' columnValues st
            state' = foldr updateRows state idsAndValues

            -- Check if the values provided are all of the same type
            typeCheckColumn typeRep values = all (\x -> typeRep == withColumnValue typeOf x) values
            valuesAreSane = all id $ map (uncurry typeCheckColumn) $ zip columnTypes columnValues
        in case valuesAreSane of
            False -> return (InconsistentTypes, Nothing)
            True -> do
              catchSTM (state' `seq` return (Ok, Just state')) (\(E.SomeException e) -> return (Fail $ show e, Nothing))

    doNewBlock blockSpec@(BlockSpec tableId columnId blockId) startRowId endRowId columnType state =
        let newBlock = ServerState.emptyBlockFromType columnType
            resizedBlock = ServerState.modifyGenericBlock (const startRowId)
                           (resize $ 1 + endRowId .- startRowId) newBlock
            newState = ServerState.establishRowMapping (tableId, columnId) (startRowId, endRowId) blockId $
                       ServerState.insertBlock blockSpec resizedBlock state
            a .- b = (fromIntegral a) - (fromIntegral b)
        in
          if ServerState.hasBlock blockSpec state then
              return (BlockAlreadyExists, Nothing)
          else
            return (Ok, Just newState)

    doBlockInfo blockSpec state =
        if ServerState.hasBlock blockSpec state then
            let blockInfo = ServerState.getBlockInfo blockSpec state
            in return (BlockInfoResponse blockInfo, Just state)
         else
            return (BlockDoesNotExist, Nothing)

    doDeleteBlock blockSpec state =
        if ServerState.hasBlock blockSpec state then
            return (Ok, Just $ ServerState.deleteBlock blockSpec state)
         else
             return (BlockDoesNotExist, Nothing)

    doMap op inputs output state =
        let allInputsExist = all (flip ServerState.hasBlock state) inputs
        in if allInputsExist then
               case ServerState.mapServerBlock op inputs output state of
                 Nothing -> return (InconsistentTypes, Nothing)
                 Just state' ->
                     let blockInfo = ServerState.getBlockInfo output state'
                     in return (MapResponse (BI.blockType blockInfo), Just state')
           else return (BlockDoesNotExist, Nothing)

    doForceComputation blockSpec state =
        if ServerState.hasBlock blockSpec state then
            return (Ok, Just $ ServerState.forceCompute blockSpec state)
        else return (BlockDoesNotExist, Nothing)

{-| Implements a simple client to the server above.
    Commands are entered directly using Haskell read syntax. Responses are parsed
    and printed using Show
-}
dataServerSimpleClient :: IO ()
dataServerSimpleClient = do
  hd <- initializeInput defaultSettings -- Initialize haskeline
  infoM moduleName "Welcome to DIME data server debug client..."
  withContext $ \c ->
      safelyWithSocket c Req (Connect "tcp://127.0.0.1:8008") $
          \s -> forever $ loop s hd
    where
      loop s hd = do
        line <- queryInput hd (getInputLine "% ")
        case line of
          Just lineData -> do
              result <- E.catch (E.evaluate $ Just $ read lineData) (\(e :: E.SomeException) -> return Nothing)
              case result of
                Nothing -> putStrLn $ "Could not parse request"
                Just (cmd :: Command) -> do
                             putStrLn $ "Sending command"
                             startTime <- getClockTime
                             sendOneRequest s cmd (putStrLn . show :: Response -> IO ())
                             endTime <- getClockTime
                             let timeTaken = endTime `diffClockTimes` startTime
                             putStrLn $ "Query took " ++ (show $ tdMin timeTaken) ++ " m, " ++ (show $ tdSec timeTaken) ++ "s, " ++ (show $ (tdPicosec timeTaken) `div` 1000000000) ++ "ms."
