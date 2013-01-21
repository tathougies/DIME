{-# LANGUAGE ScopedTypeVariables #-}
module Database.DIME.DataServer
    (
     dataServerMain, dataServerSimpleClient
    ) where
import Database.DIME
import qualified Database.DIME.DataServer.State as ServerState
import Database.DIME.DataServer.Command
import Database.DIME.DataServer.Response
import Database.DIME.Util
import Database.DIME.Memory.Block
import qualified Database.DIME.Memory.BlockInfo as BI

import System.Log.Logger
import System.Console.Haskeline
import System.Console.Haskeline.IO
import System.IO

import qualified  Control.Exception as E
import Control.Monad
import Control.Concurrent
import Control.Concurrent.SampleVar

import GHC.Conc (numCapabilities)

import qualified System.ZMQ3 as ZMQ
import qualified Data.ByteString as SB
import qualified Data.ByteString.Lazy as LBS
import Data.Binary
import Data.String
import Data.List
import Data.Function
import Data.Typeable

import qualified Database.DIME.Server.Peers as Peers

-- For module use only
moduleName = "Database.DIME.DataServer"

keepAliveTime = 10000000

dataServerMain :: String -> String -> IO ()
dataServerMain coordinatorName localAddress = do
  infoM moduleName "DIME Data server starting..."
  ZMQ.withContext $ \c -> do
      infoVar <- newSampleVar ServerState.empty
      forkIO $ statusClient infoVar coordinatorName localAddress c
      ZMQ.withSocket c ZMQ.Rep $ \s -> do
          ZMQ.bind s "tcp://127.0.0.1:8008"
          runStatefulIOLoop ServerState.empty $ loop s infoVar -- Run the loop with this state.
  return ()
  where
    statusClient infoVar coordinatorName hostName ctxt =
        ZMQ.withSocket ctxt ZMQ.Req $ \s ->
            do
              ZMQ.connect s coordinatorName
              let zmqName = Peers.PeerName $ "tcp://" ++ hostName ++ ":8008"
              forever $ do
                   putStrLn "Fetch state"
                   state <- readSampleVar infoVar
                   putStrLn $ show $ ServerState.getBlocks state
                   let cmdData = Peers.mkUpdateCommand zmqName $ ServerState.getBlockCount state
                   E.evaluate cmdData
                   putStrLn "Fetch state 2"
                   ZMQ.send s [] cmdData
                   reply <- ZMQ.receive s
                   let response = Peers.parsePeerResponse reply
                   case response of
                     Peers.InfoRequest -> return ()
                     _ -> return ()
                   threadDelay keepAliveTime

    loop s infoVar state = do
      line <- ZMQ.receive s
      let cmd :: Command
          cmd = decode $ LBS.fromChunks [line]
      (response, newState) <- case cmd of -- Parse the command
                                FetchRows tableId rowIds columnIds ->
                                    doFetchRows tableId rowIds columnIds state
                                UpdateRows tableId rowIds columnIds values ->
                                    doUpdateRows tableId rowIds columnIds values state
                                -- Block commands
                                NewBlock tableId columnId blockId startRowId endRowId columnType ->
                                    doNewBlock tableId columnId blockId startRowId endRowId columnType state
                                DeleteBlock tableId columnId blockId ->
                                    doDeleteBlock tableId columnId blockId state
                                BlockInfo tableId columnId blockId ->
                                    doBlockInfo tableId columnId blockId state
      ZMQ.send s [] $ head $ LBS.toChunks $ encode response
      writeSampleVar infoVar state
      case newState of
        Just state' -> return state'
        Nothing -> return state

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
                   putStrLn $ show idsAndValues
                   result <- E.try (E.evaluate state') :: IO (Either SomeException ServerState.DataServerState)
                   return $ case result of
                              Left e -> (Fail $ show e, Nothing)
                              Right state -> (Ok, Just state)

    doNewBlock tableId columnId blockId startRowId endRowId columnType state =
        let newBlock = ServerState.emptyBlockFromType columnType
            resizedBlock = ServerState.modifyGenericBlock (const startRowId)
                           (resize $ 1 + endRowId .- startRowId) newBlock
            newState = ServerState.establishRowMapping (tableId, columnId) (startRowId, endRowId) blockId $
                       ServerState.insertBlock tableId columnId blockId resizedBlock state
            a .- b = (fromIntegral a) - (fromIntegral b)
        in
          if ServerState.hasBlock tableId columnId blockId state then
              return (BlockAlreadyExists, Nothing)
          else
            return (Ok, Just newState)

    doBlockInfo tableId columnId blockId state =
        if ServerState.hasBlock tableId columnId blockId state then
            let blockInfo = ServerState.getBlockInfo tableId columnId blockId state
            in putStrLn "Do block info" >> return (BlockInfoResponse blockInfo, Just state)
         else
            return (BlockDoesNotExist, Nothing)

    doDeleteBlock tableId columnId blockId state =
        if ServerState.hasBlock tableId columnId blockId state then
            return (Ok, Just $ ServerState.deleteBlock tableId columnId blockId state)
         else
             return (BlockDoesNotExist, Nothing)

{-| Implements a simple client to the server above.
    Commands are entered directly using Haskell read syntax. Responses are parsed
    and printed using Show
-}
dataServerSimpleClient :: IO ()
dataServerSimpleClient = do
  hd <- initializeInput defaultSettings -- Initialize haskeline
  infoM moduleName "Welcome to DIME data server debug client..."
  ZMQ.withContext $ \c ->
      ZMQ.withSocket c ZMQ.Req $ \s -> do
          ZMQ.connect s "tcp://127.0.0.1:8008"
          forever $ loop s hd
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
                             let cmdData = head $ LBS.toChunks $ encode cmd
                             ZMQ.send s [] cmdData
                             reply <- ZMQ.receive s
                             let response = (decode $ LBS.fromChunks [reply] :: Response)
                             putStrLn $ show response
