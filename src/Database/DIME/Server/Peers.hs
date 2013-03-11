{-# LANGUAGE GeneralizedNewtypeDeriving, RecordWildCards, TupleSections, RecordWildCards #-}
module Database.DIME.Server.Peers
    ( PeerCommand(..),
      PeerResponse(..),
      peerServer
    ) where

import Control.Concurrent.STM
import Control.Monad

import qualified Data.Map as Map
import qualified Data.PSQueue as PSQ
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Tree.DisjointIntervalTree as DIT
import Data.Binary as B
import Data.Maybe
import Data.String
import Data.Int

import Database.DIME
import Database.DIME.Transport
import Database.DIME.Util
import Database.DIME.Server.State
import Database.DIME.Server.Config
import Database.DIME.DataServer.Command
import Database.DIME.Memory.Block (ColumnType(..))

import System.Time
import System.Log.Logger

import qualified Text.JSON as JSON

moduleName = "Database.DIME.Server.Peers"

data PeerCommand = UpdateInfo PeerName Int |
                   TimeSeriesInfo QueryKey TimeSeriesName |
                   TimeSeriesColumnInfo QueryKey TimeSeriesName ColumnName
                 deriving (Show, Eq)

data PeerResponse = Ok | InfoRequest |
                    ObjectNotFound |
                    TimeSeriesInfoResponse {
                        tableName :: TimeSeriesName,
                        tableId :: TableID,
                        tableLength :: TimeSeriesIx,
                        columnNamesAndTypes :: [(ColumnName, ColumnType)],
                        startTime :: ClockTime,
                        dataFrequency :: Int64
                    } |
                    TimeSeriesColumnInfoResponse {
                        columnName :: ColumnName,
                        columnId :: ColumnID,
                        blocks :: [(BlockID, [PeerName])],
                        rowMappings :: [((Int64, Int64), BlockID)]}

-- | The peer server. This runs on the main server and keeps track of the peer state.
peerServer :: State -> IO ()
peerServer serverState = do
  infoM moduleName "DIME peer server starting"
  withContext $ \c ->
      safelyWithSocket c Rep (Bind $ "tcp://*:" ++ show coordinatorPort) $
            (forever . serve serverState)
  where
    serve serverState s =
        serveRequest s (return ()) $
            \cmd -> case cmd of
                      UpdateInfo peerName blockCount -> doUpdateInfo peerName blockCount serverState
                      TimeSeriesInfo _ tsName -> doTimeSeriesInfo tsName serverState -- query key ignored for now
                      TimeSeriesColumnInfo _ tsName columnName -> doTimeSeriesColumnInfo tsName columnName serverState

    doUpdateInfo peerName blockCount serverState = do
      peerAdded <- atomically $ do
        let peersInfoVar = peersRef serverState
        peersInfo <- readTVar peersInfoVar

        -- Update or add the peer
        let peers = getPeers peersInfo
        (peerAdded, peerInfoVar) <- case Map.lookup peerName peers of
                         Just peerInfoVar -> do -- If it already exists, just update it
                             modifyTVar' peerInfoVar
                                         (\peerInfo -> peerInfo { getBlockCount = blockCount })
                             return (False, peerInfoVar)
                         Nothing -> do -- Make new TVar for this peer
                             peerVar <- newTVar $ PeerInfo { getPeerName = peerName,
                                                             getBlockCount = blockCount }
                             let peers' = Map.insert peerName peerVar peers
                                 peersInfo' = peersInfo { getPeers = peers' }
                             writeTVar peersInfoVar peersInfo'
                             return (True, peerVar)

        -- Update its placement in the priority queue
        peerData <- readTVar peerInfoVar
        let peerPriority = calcPeerPriority peerData
            peersUsage' = PSQ.alter (Just . const peerPriority) peerName $ getPeersUsage peersInfo
        modifyTVar' peersInfoVar (\x -> x { getPeersUsage = peersUsage' })

        return peerAdded
      when peerAdded (infoM moduleName $ "Added peer " ++ show peerName)
      if peerAdded
       then return InfoRequest
       else return Ok

    doTimeSeriesColumnInfo tsName columnName serverState = atomically $ do
      cRes <- lookupColumn tsName columnName serverState
      case cRes of
        Nothing -> return ObjectNotFound
        Just cVar -> do {- Return column info -}
                  column <- readTVar cVar
                  let blocks = Map.assocs $ getBlocks column
                      blocks' = map (\(bId, bData) -> (bId, getOwners bData)) blocks
                      rowMappings = DIT.assocs $ getBlockRanges column
                  return $ TimeSeriesColumnInfoResponse {
                               columnName = getColumnName column,
                               columnId = getColumnId column,
                               blocks = blocks',
                               rowMappings = rowMappings}

    doTimeSeriesInfo tsName serverState = atomically $ do
      tsRes <- lookupTimeSeries tsName serverState
      case tsRes of
        Nothing -> return ObjectNotFound
        Just tsVar -> do {- Return time series information -}
                   ts <- readTVar tsVar
                   let columnInfo = Map.assocs $ getColumns ts
                   columnNamesAndTypes <-
                       mapM (\(name, columnVar) ->
                                 liftM ((name,) . getColumnType) $ readTVar columnVar) columnInfo
                   return $ TimeSeriesInfoResponse {
                           tableName = getName ts,
                           tableId = getTableId ts,
                           tableLength = getLength ts,
                           columnNamesAndTypes = columnNamesAndTypes,
                           startTime = getStartTime ts,
                           dataFrequency = getDataFrequency ts}

    calcPeerPriority = getBlockCount -- this is the function you'd change to give a new heuristic to how peers are ranked

instance Binary PeerCommand where
    get = do
      tag <- (get :: Get Int8)
      case tag of
        1 {- UpdateInfo -} -> liftM2 UpdateInfo get get
        2 {- TimeSeriesInfo -} -> liftM2 TimeSeriesInfo get get
        3 {- TimeSeriesColumnInfo -} -> liftM3 TimeSeriesColumnInfo get get get

    put (UpdateInfo peerName blockCount) = do
      put (1 :: Int8)
      put peerName
      put blockCount
    put (TimeSeriesInfo key tsName) = do
      put (2 :: Int8)
      put key
      put tsName
    put (TimeSeriesColumnInfo key tsName columnName) = do
      put (3 :: Int8)
      put key
      put tsName
      put columnName

instance Binary PeerResponse where
    get = do
      token <- (get :: Get Int8)
      case token of
        1 -> return Ok
        2 -> return InfoRequest
        3 -> return ObjectNotFound
        4 -> doTimeSeriesResponse
        5 -> doTimeSeriesColumnResponse
        _ -> fail "Bad code for PeerResponse"
     where
       doTimeSeriesResponse = do
           tableName <- get
           tableId <- get
           tableLength <- get
           columnNamesAndTypes <- get
           startTime <- get
           dataFrequency <- get
           return $ TimeSeriesInfoResponse {
                              tableName = tableName,
                              tableId = tableId,
                              tableLength = tableLength,
                              columnNamesAndTypes = columnNamesAndTypes,
                              startTime = startTime,
                              dataFrequency = dataFrequency}
       doTimeSeriesColumnResponse = do
           columnName <- get
           columnId <- get
           blocks <- get
           rowMappings <- get
           return $ TimeSeriesColumnInfoResponse {..}
    put Ok = put (1 :: Int8)
    put InfoRequest = put (2 :: Int8)
    put ObjectNotFound = put (3 :: Int8)
    put (TimeSeriesInfoResponse {..}) = do
        put (4 :: Int8)
        put tableName
        put tableId
        put tableLength
        put columnNamesAndTypes
        put startTime
        put dataFrequency
    put (TimeSeriesColumnInfoResponse {..}) = do
        put (5 :: Int8)
        put columnName
        put columnId
        put blocks
        put rowMappings