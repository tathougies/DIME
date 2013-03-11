{-# LANGUAGE GeneralizedNewtypeDeriving, ScopedTypeVariables #-}
module Database.DIME.Server.State
    ( State(..), TimeSeriesName (..), ColumnName (..),
      TimeSeriesData(..), ColumnData(..), PeerName(..),
      PeersInfo(..), PeerInfo(..), TimeSeriesIx(..),
      BlockData(..),
      newServerState, newTimeSeries, newColumn,
      lookupTimeSeries, lookupColumn,
      deleteColumn, appendRow,
      timeSeriesAsJSON, columnAsJSON,
      jsValueToColumnValue,
      rebuildTimeSeriesMap
    ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Control.Monad

import qualified Data.Map as Map
import qualified Data.PSQueue as PSQ
import Data.PSQueue (Binding ((:->)))
import qualified Data.Text as Text
import qualified Data.Vector as V
import qualified Data.Tree.DisjointIntervalTree as DIT
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as Bin
import qualified Data.Set as Set
import Data.Maybe
import Data.Int
import Data.String
import Data.Ratio
import Data.List
import Data.Ord

import Debug.Trace

import qualified Database.DIME.Memory.Block as B
import Database.DIME.Util
import Database.DIME.Server.Util
import Database.DIME.DataServer.Response hiding (Ok)
import Database.DIME.DataServer.Command
import Database.DIME.Transport
import Database.DIME

import System.Time
import System.Directory
import System.Log.Logger
import System.Locale

import Text.JSON

moduleName = "Database.DIME.Server.State"
blockSize = 65536

-- | Data type for ZeroMQ peer name
newtype PeerName = PeerName String
    deriving (Show, Ord, Eq, IsString, JSON, Bin.Binary)
-- | Data type of an index into a time series
newtype TimeSeriesIx = TimeSeriesIx Int64
    deriving (Show, Ord, Eq, Num, Enum, JSON, Bin.Binary, Integral, Real)
-- | Data type of a time series name
newtype TimeSeriesName = TimeSeriesName Text.Text
    deriving (Show, Ord, Eq, IsString, JSON, Bin.Binary)
-- | Data type of a column name
newtype ColumnName = ColumnName Text.Text
    deriving (Show, Ord, Eq, IsString, JSON, Bin.Binary)

data PeerInfo = PeerInfo {
      getPeerName :: PeerName,
      getBlockCount :: Int -- The number of blocks that this peer is handling
    }

data PeersInfo = PeersInfo {
        getPeers :: Map.Map PeerName (TVar PeerInfo),
        getPeersUsage :: PSQ.PSQ PeerName Int
      }

data BlockData = BlockData {
      getOwners :: [PeerName]
    } deriving Show

data ColumnData = ColumnData {
      getColumnName :: ColumnName,
      getColumnId :: ColumnID,
      getColumnType :: B.ColumnType,
      getBlocks :: Map.Map BlockID BlockData,
      getAllocatedBlocks :: DIT.DisjointIntervalTree Int64 BlockID,
      getBlockRanges :: DIT.DisjointIntervalTree Int64 BlockID
    } deriving Show

type Column = TVar ColumnData

data TimeSeriesData = TimeSeriesData {
      getName :: TimeSeriesName,
      getTableId :: TableID,
      getLength :: TimeSeriesIx,
      getColumns :: Map.Map ColumnName Column,
      getStartTime :: ClockTime,
      getDataFrequency :: Int64
    }

type TimeSeries = TVar TimeSeriesData

data State = ServerState {
      configLocation :: FilePath,
      timeSeriessRef :: TVar (Map.Map TimeSeriesName TimeSeries),
      peersRef :: TVar PeersInfo,
      zmqContext :: Context
    }

calcMeanBlockCount :: Floating a => PeersInfo -> IO a
calcMeanBlockCount state = do
    peers <- mapM readTVarIO $ Map.elems $ getPeers state
    let peerBlockCount = map (fromIntegral.getBlockCount) peers
    return $ (sum peerBlockCount) / (fromIntegral $ length peerBlockCount)

calcBlockCountStDev :: Floating a => PeersInfo -> IO a
calcBlockCountStDev state = do
    peers <- mapM readTVarIO $ Map.elems $ getPeers state
    let peerBlockCount = map (fromIntegral.getBlockCount) peers
    meanBlockCount <- calcMeanBlockCount state
    let variance = sum (map (\x -> (x - meanBlockCount) * (x - meanBlockCount)) peerBlockCount) /
                   (fromIntegral $ length peerBlockCount)
    return $ sqrt variance

getLeastUtilizedPeer :: PeersInfo -> IO (Maybe PeerName)
getLeastUtilizedPeer peersInfo = do
  let peersUsage = getPeersUsage peersInfo
      peers = getPeers peersInfo
  case PSQ.findMin peersUsage of
    Nothing -> return Nothing
    Just (k :-> p) -> do
      let peerVar = fromJust $ Map.lookup k peers
      peerData <- readTVarIO peerVar
      return $ Just $ getPeerName peerData

newServerState :: FilePath -> IO State
newServerState configLocation = do
  timeSeriessTVar <- newTVarIO Map.empty
  peersTVar <- newTVarIO $ PeersInfo Map.empty PSQ.empty
  ctxt <- initTransport
  let baseState = ServerState configLocation timeSeriessTVar peersTVar ctxt
  readStateFromConfigFile baseState

  where
    readStateFromConfigFile state =
        do
          fileExists <- doesFileExist configLocation
          if fileExists
           then do
             -- Read the file
             fileContents <- readFile configLocation
             let timeSeriess = decode fileContents
             case timeSeriess of
               Error e -> error "Couldn't read file"
               Ok (JSObject timeSeriess') ->
                         do
                           timeSeriessVarData <- recreateTimeSeriess =<< (mapMaybeM parseSeries $ fromJSObject timeSeriess')
                           atomically $ writeTVar (timeSeriessRef state) timeSeriessVarData
                           return state
               Ok x -> error $ "Couldn't read file. Got " ++ show x
            else return state

    parseSeries :: (String, JSValue) -> IO (Maybe (String, TimeSeriesData))
    parseSeries (name, unparsedData) =
        do
          parsed <- parseSeriesData unparsedData
          case parsed of
            Error e -> do
                        errorM moduleName $ "Error parsing series " ++ name ++ " : " ++ e
                        return Nothing
            Ok x -> return $ Just (name, x)

    parseSeriesData :: JSValue -> IO (Result TimeSeriesData)
    parseSeriesData (JSObject timeSeriesData') =
        let timeSeriesData = fromJSObject timeSeriesData'
            l fieldName = lookup fieldName timeSeriesData
        in
          case (l "name", l "tableId", l "length", l "columns", l "startTime", l "dataFrequency") of
            (Just nameD, Just tableIdD, Just lengthD, Just columnsD, Just startTimeD,
                  Just dataFrequencyD) ->
                case (readJSON nameD, readJSON tableIdD, readJSON lengthD, readJSON columnsD,
                      readJSON startTimeD, readJSON dataFrequencyD) of
                  (Ok name, Ok tableId, Ok length, Ok (JSObject columns), Ok startTime, Ok dataFrequency) ->
                      do
                        columnsData <- mapM
                                           (\(name, column) -> do
                                              let columnData = readJSON column :: Result ColumnData
                                              case columnData of
                                                Error e -> do
                                                  errorM moduleName $ "Error reading columns: " ++ e
                                                  fail "Error reading column"
                                                Ok columnData ->
                                                    do
                                                      var <- atomically $ newTVar $ columnData
                                                      return $ (ColumnName $ fromString name, var)) $ fromJSObject columns
                        let columnsMap = Map.fromList columnsData
                        return $ Ok $ TimeSeriesData name tableId length columnsMap startTime dataFrequency
                  _ -> return $ Error "Bad data types for TimeSeriesData"
            _ -> return $ Error "Bad object structure for TimeSeriesData"
    parseSeriesData _ = return $ Error "Bad format for TimeSeriesData"

    recreateTimeSeriess :: [(String, TimeSeriesData)] -> IO (Map.Map TimeSeriesName TimeSeries)
    recreateTimeSeriess timeSeriess =
        do
          timeSeriessData <- atomically $ mapM recreateTimeSeries timeSeriess
          return $ Map.fromList timeSeriessData

    recreateTimeSeries (name, timeSeries) =
        do
          timeSeriesVar <- newTVar $ timeSeries
          return (TimeSeriesName $ fromString name, timeSeriesVar)

rebuildTimeSeriesMap :: State -> IO ()
rebuildTimeSeriesMap state = do
  timeSeriessData <- atomically $ -- Get all the timeSeries in an understandable form
                 do
                   timeSeriess <- readTVar $ timeSeriessRef state
                   (liftM (JSObject . toJSObject)) $ mapM (\(TimeSeriesName name, ts) -> do
                            encodedTS <- timeSeriesAsJSValue ts
                            return (Text.unpack name, encodedTS)) $ Map.toList timeSeriess
  writeFile (configLocation state) $
            encode timeSeriessData
  return ()

newTimeSeries :: TimeSeriesName -> Int64 -> State -> IO TimeSeries
newTimeSeries name frequency state@(ServerState { timeSeriessRef = timeSeriessRef }) =
    do
      localTime <- getClockTime
      timeSeriesRet <- atomically $ do
                         timeSeriess <- readTVar timeSeriessRef
                         case Map.lookup name timeSeriess of
                           Just x -> {- already added -} return x
                           Nothing -> do
                                      maxId <- case Map.size timeSeriess of
                                                 0 -> return $ TableID 1
                                                 _ -> liftM (getTableId .
                                                             maximumBy (comparing getTableId)) $
                                                      mapM readTVar $ Map.elems timeSeriess
                                      let newId = maxId + (TableID 1)
                                      timeSeries <- newTimeSeries' name newId frequency localTime
                                      let timeSeriess' = Map.insert name timeSeries timeSeriess -- add to map
                                      writeTVar timeSeriessRef timeSeriess'
                                      return timeSeries

      -- After updating the in-memory representation, persist it to the file
      rebuildTimeSeriesMap state -- Rewrites the time series configuration file

      return timeSeriesRet
    where
      newTimeSeries' name tableId frequency localTime =
          newTVar $ TimeSeriesData name tableId 0 Map.empty localTime frequency

newColumn :: TimeSeriesName -> ColumnName -> B.ColumnType -> State -> IO Column
newColumn tsName name columnType state =
    do
      columnVar <- atomically $ do
                     timeSeriess <- readTVar $ timeSeriessRef state
                     case Map.lookup tsName timeSeriess of
                       Just timeSeriesVar -> do
                           timeSeries <- readTVar timeSeriesVar
                           let columns = getColumns timeSeries
                               res = Map.lookup name columns
                           case res of
                             Nothing ->
                                 do
                                   maxId <- case Map.size columns of
                                              0 -> return $ ColumnID 1
                                              _ -> liftM (getColumnId .
                                                          maximumBy (comparing getColumnId)) $
                                                   mapM readTVar $ Map.elems columns
                                   let newId = maxId + (ColumnID 1)
                                   column <- newColumn' name newId columnType
                                   let columns' = Map.insert name column columns
                                       timeSeries' = timeSeries { getColumns = columns' }
                                   writeTVar timeSeriesVar timeSeries' -- Update time series structure
                                   return column
                             Just columnVar -> return columnVar
                       Nothing -> error $ "No such time series " ++ show tsName

      -- persist state to the file
      rebuildTimeSeriesMap state

      return columnVar
    where
      newColumn' name columnId columnType = newTVar $ ColumnData name columnId columnType Map.empty DIT.empty DIT.empty

deleteColumn :: TimeSeriesName -> ColumnName -> State -> IO ()
deleteColumn timeSeriesName columnName state =
    do
      timeSeriesR <- atomically $ lookupTimeSeries timeSeriesName state
      case timeSeriesR of
        Nothing -> return ()
        Just timeSeries -> do
            atomically $ do
              timeSeriesData <- readTVar timeSeries
              let timeSeriesData' = timeSeriesData { getColumns = Map.delete columnName $ getColumns timeSeriesData }
              writeTVar timeSeries timeSeriesData'
              return ()

            rebuildTimeSeriesMap state -- persist change
            return ()

lookupTimeSeries :: TimeSeriesName -> State -> STM (Maybe TimeSeries)
lookupTimeSeries name (ServerState { timeSeriessRef = timeSeriessRef }) =
    do
      timeSeriess <- readTVar timeSeriessRef
      return $ Map.lookup name timeSeriess

lookupColumn :: TimeSeriesName -> ColumnName -> State -> STM (Maybe Column)
lookupColumn timeSeriesName columnName state =
    do
      timeSeriesR <- lookupTimeSeries timeSeriesName state
      case timeSeriesR of
        Nothing -> return Nothing
        Just timeSeries ->
            do
              timeSeriesData <- readTVar timeSeries
              let column = Map.lookup columnName $ getColumns timeSeriesData
              return column

appendRow :: TimeSeriesName -> [(ColumnName, B.ColumnValue)] -> State -> IO (Maybe Int)
appendRow timeSeriesName columnsAndValues state =
    do
      infoM moduleName $ "Appending row " ++ show columnsAndValues
      timeSeriesR <- atomically $ lookupTimeSeries timeSeriesName state
      case timeSeriesR of
        Just timeSeries ->
            do
              timeSeriesData <- readTVarIO timeSeries
              timeStamp <- getClockTime
              let numSecondsSinceStartTime = floor $ toRational $ tdSec $ timeStamp `diffClockTimes` startTime
                  index = numSecondsSinceStartTime `div` dataFrequency
                  context = zmqContext state
                  startTime = getStartTime timeSeriesData
                  dataFrequency = getDataFrequency timeSeriesData

              mVars <- replicateM (length columnsAndValues) (newEmptyMVar :: IO (MVar Bool))
              mapM (forkIO . (uncurry $ appendColumnInRow state timeSeries index)) $ zip mVars columnsAndValues  -- TODO figure out error handling, etc)
              success <- mapM takeMVar mVars -- Wait on all MVars... ie wait for all the threads to complete
              if (all id success)
               then atomically $
                   do
                     -- Everything was successful, update the time series object
                     timeSeriesData <- readTVar timeSeries
                     let timeSeriesData' = timeSeriesData { getLength = fromIntegral index + 1 }
                     writeTVar timeSeries timeSeriesData'
                     return $ Just 0
               else
                  return Nothing
        Nothing -> return Nothing
    where
      appendColumnInRow :: State -> TimeSeries -> Int64 -> MVar Bool -> (ColumnName, B.ColumnValue) -> IO ()
      appendColumnInRow state timeSeries rowIndex signalVar (columnName, columnValue) =
          do
            infoM moduleName $ "Appending column in row " ++ show rowIndex
            let context = zmqContext state
            -- Figure out if new blocks need to be allocated, and dispatch the appropriate request to the appropriate server
            timeSeriesData <- readTVarIO timeSeries
            let column = fromJust $ Map.lookup columnName $ getColumns timeSeriesData

            columnData <- readTVarIO column
            let blockRanges = getBlockRanges columnData
                allocatedBlocks = getAllocatedBlocks columnData
                blocks = getBlocks columnData
                tableId = getTableId timeSeriesData
                columnId = getColumnId columnData

            (blockId, blockData) <- case DIT.lookup rowIndex allocatedBlocks of
                           Just x -> return (x, fromJust $ Map.lookup x blocks) -- We found the block, send the add message and continue
                           Nothing -> do
                             bestPeerRes <- calculateBestPeer state columnData
                             case bestPeerRes of
                               Just bestPeerName -> do
                                                let maxBlockId = case Map.size $ getBlocks columnData of
                                                                   0 -> BlockID 0
                                                                   _ -> maximum $ Map.keys $ getBlocks columnData
                                                    newBlockId = maxBlockId + (BlockID 1)

                                                (startRowId, endRowId) <- createBlockOnPeer context bestPeerName tableId columnId
                                                                          newBlockId (fromIntegral rowIndex) (getColumnType columnData)

                                                let newBlockData = BlockData { getOwners = [bestPeerName] }

                                                -- Add this block data to the column
                                                atomically $ modifyTVar' column
                                                    (\columnData ->
                                                         columnData {
                                                           getBlocks = Map.insert newBlockId newBlockData $ getBlocks columnData,
                                                           getAllocatedBlocks = DIT.insert (startRowId, endRowId) newBlockId $ getAllocatedBlocks columnData
                                                         })

                                                return (newBlockId, newBlockData)
                               Nothing -> do
                                        putMVar signalVar False
                                        fail "Could not find peer to place block on"
            infoM moduleName $ "Will put in " ++ show blockId ++ " on peers " ++ show blockData
            -- Now send a message out to each owner adding the value
            results <- forM (getOwners blockData)
                  (\(PeerName peer) ->
                       let appendCmd = UpdateRows tableId [RowID $ fromIntegral rowIndex] [columnId] [[columnValue]]
                       in sendRequest context (Connect peer) appendCmd $
                          (return . not . isFailure)
                  )
            if (all id results)
             then do
               -- The addition was successful, we update the column correspondingly
               atomically $ modifyTVar' column
                          (\columnData ->
                               columnData { getBlockRanges = DIT.insert (rowIndex, succ rowIndex) blockId $ getBlockRanges columnData })
               putMVar signalVar True
             else putMVar signalVar False -- failure :(
            return ()

      createBlockOnPeer :: Context -> PeerName -> TableID -> ColumnID -> BlockID -> Int64 -> B.ColumnType -> IO (Int64, Int64)
      createBlockOnPeer ctxt (PeerName peer) tableId columnId blockId rowId columnType =
          let beginRowId = RowID $ rowId - rowId `mod` blockSize
              endRowId = beginRowId + (RowID blockSize)
              newBlockCommand = NewBlock (BlockSpec tableId columnId blockId) beginRowId endRowId columnType
          in sendRequest ctxt (Connect peer) newBlockCommand $
             (\(response :: Response) -> do
                  when (isFailure response) $
                       warningM moduleName ("Failed creating block " ++ show blockId ++ " on " ++ peer)
                  return (fromIntegral beginRowId, fromIntegral endRowId))

      calculateBestPeer :: State -> ColumnData -> IO (Maybe PeerName)
      calculateBestPeer state columnData = do
        peersData <- readTVarIO $ peersRef state
        let peersInfo = getPeers peersData
            ownersList = map getOwners $ Map.elems $ getBlocks columnData -- get all
            preferredPeersList = concat ownersList
            preferredPeerNames = nub preferredPeersList -- TODO this messes up the order of the list

        preferredPeers <- mapM (\name -> readTVarIO $ fromJust $ Map.lookup name peersInfo) preferredPeerNames

        let peerDistanceMap = Map.fromList $ zip preferredPeerNames $ reverse [0..(length preferredPeers) - 1]
            getPeerDistance peerName = fromJust $ Map.lookup peerName peerDistanceMap

            -- consider each peer and calculate the distance as well as the number of blocks available
            -- We only consider peers that are within one half standard deviation of the mean block count
        meanBlockCount <- (calcMeanBlockCount peersData :: IO Double)
        blockCountStDev <- (calcBlockCountStDev peersData :: IO Double)

        let considerablePeers = filter (\peer -> let blockCount = fromIntegral $ getBlockCount peer
                                                     in blockCount >= (meanBlockCount - blockCountStDev / 2) &&
                                                        blockCount <= (meanBlockCount + blockCountStDev / 2))
                                preferredPeers

        -- Calculate a heuristic for each of the considerable peers.
        -- The peer with the highest number wins. If there are no considerable peers, find
        -- the overall least utilized peer
        case considerablePeers of
          [] -> getLeastUtilizedPeer peersData
          _ -> do
            let scorePeer peer = negate $ (getPeerDistance $ getPeerName peer) + (getBlockCount peer)
                peerScores = map scorePeer considerablePeers
                bestPeer = snd $ maximumBy (comparing fst) $ zip peerScores considerablePeers
            return $ Just $ getPeerName bestPeer

getPeersForRange :: ClockTime -> ClockTime -> State -> STM [[PeerName]]
getPeersForRange startTime endTime state = return [] -- TODO

-- JSON Serialization functions and instances

timeSeriesAsJSValue :: TimeSeries -> STM JSValue
timeSeriesAsJSValue tsVar = do
  ts <- readTVar tsVar
  columns <- liftM toJSObject $ mapM
             (\(ColumnName name, var) -> do
                column <- readTVar var
                return (Text.unpack name, showJSON column)) $
             Map.toList $ getColumns ts
  return $ showJSON $ toJSObject [
                             ("name", showJSON $ getName ts),
                             ("tableId", showJSON $ getTableId ts),
                             ("length", showJSON $ getLength ts),
                             ("columns", showJSON $ columns),
                             ("startTime", showJSON $ getStartTime ts),
                             ("dataFrequency", showJSON $ getDataFrequency ts)
                            ]

columnAsJSON :: IsString a => Column -> STM a
columnAsJSON c = do
  column <- readTVar c
  return $ fromString $ encode column

timeSeriesAsJSON :: IsString a => TimeSeries -> STM a
timeSeriesAsJSON timeSeries = do
  timeSeriesJSValue <- timeSeriesAsJSValue timeSeries
  return $ fromString $ encode timeSeriesJSValue

jsValueToColumnValue :: B.ColumnType -> JSValue -> Maybe B.ColumnValue
jsValueToColumnValue B.IntColumn (JSRational _ rational) = Just $ B.ColumnValue $ ((fromIntegral $ numerator rational `div`  (denominator rational)) :: Int)
jsValueToColumnValue B.DoubleColumn (JSRational _ rational) = Just $ B.ColumnValue $ (fromRational rational :: Double)
jsValueToColumnValue B.StringColumn (JSString s) = Just $ B.ColumnValue $ fromJSString s
jsValueToColumnValue _ _ = Nothing

instance JSON ClockTime where
    showJSON (TOD seconds picoseconds) = JSObject $ toJSObject [
                                                ("seconds", showJSON seconds),
                                                ("picoseconds", showJSON picoseconds)
                                               ]
    readJSON (JSObject o) = let parsed = fromJSObject o
                                secondsR = lookup "seconds" parsed
                                picosecondsR = lookup "picoseconds" parsed
                            in
                              case (secondsR, picosecondsR) of
                                (Just seconds, Just picoseconds) ->
                                    let secondsInt = readJSON seconds :: Result Integer
                                        picosecondsInt = readJSON picoseconds :: Result Integer
                                    in case (secondsInt, picosecondsInt) of
                                         (Ok secondsParsedInt, Ok picosecondsParsedInt) -> Ok $ TOD secondsParsedInt picosecondsParsedInt
                                         (_,_) -> Error "invalid types for object fields in parsing ClockTime"
                                _ -> Error "Invalid object fields for ClockTime. Expecting seconds and picoseconds"
    readJSON _ = Error "Invalid JSON for ClockTime"

instance JSON BlockData where
    showJSON x = JSObject $ toJSObject [
                  ("peers", showJSON $ getOwners x)
                 ]
    readJSON a = case readJSON a of
                   Error x -> Error $ "Invalid JSON" ++ x
                   Ok assoc ->
                       case lookup "peers" (fromJSObject assoc) of
                         Nothing -> Error "Invalid structure for BlockData object"
                         Just (JSArray peerData) ->
                             let parsePeers [] = []
                                 parsePeers (x:xs) = case readJSON x of
                                                       Error _ -> parsePeers xs
                                                       Ok a -> a : parsePeers xs
                             in Ok $ BlockData $ parsePeers peerData
                         Just _ -> Error "Could not read array"

instance (Ord k, Eq v, JSON k, JSON v) => JSON (DIT.DisjointIntervalTree k v) where
    showJSON = showJSON . DIT.assocs
    readJSON assocs = case readJSON assocs of
                        Error e -> trace e $ Error e
                        Ok assocList -> Ok $ DIT.fromList assocList -- foldr (uncurry DIT.insert) DIT.empty assocList

instance JSON ColumnData where
    showJSON x = showJSON $ toJSObject [
                          ("name", showJSON $ getColumnName x),
                          ("columnId", showJSON $ getColumnId x),
                          ("type", showJSON $ getColumnType x),
                          ("blocks", showJSON $ getBlocks x),
                          ("ranges", showJSON $ getBlockRanges x),
                          ("allocatedBlocks", showJSON $ getAllocatedBlocks x)
                         ]
    readJSON (JSObject objData') =
        let objData = fromJSObject objData'
            l fieldName = lookup fieldName objData -- some shorthand
        in
          case (l "name", l "columnId", l "blocks", l "type", l "allocatedBlocks", l "ranges") of
            (Just nameD, Just columnIdD, Just blocksD, Just typeD, Just allocatedBlocksD,
             Just blockRangesD) ->
                case (readJSON nameD, readJSON columnIdD, readJSON blocksD, readJSON typeD,
                      readJSON allocatedBlocksD, readJSON blockRangesD) of
                  (Ok name, Ok columnId, Ok blocks, Ok columnType, Ok allocatedBlocks, Ok blockRanges) ->
                      Ok $ ColumnData name columnId columnType blocks allocatedBlocks blockRanges
                  _ -> Error "Bad types for ColumnData"
            _ -> Error "Bad object structure for ColumnData"
    readJSON _ = Error "Bad format for ColumnData"

instance JSON a => JSON (V.Vector a) where
    showJSON = showJSON.(V.toList)
    readJSON a = case readJSON a of
                   Ok x -> Ok $ V.fromList x
                   Error e -> Error e