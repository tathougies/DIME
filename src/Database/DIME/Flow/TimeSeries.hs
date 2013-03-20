{-# LANGUAGE TupleSections, ViewPatterns, RecordWildCards #-}
module Database.DIME.Flow.TimeSeries where

import Control.Monad.Trans
import Control.Monad
import Control.Arrow ((&&&))

import qualified Data.Map as M
import qualified Data.Tree.DisjointIntervalTree as DIT
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as Bin
import qualified Data.Tuple as Tuple
import Data.Text as T hiding (map, intercalate, all, head, foldl,
                              length, zip, splitAt, zipWith, concat,
                              last, groupBy, transpose, concatMap, group,
                              scanl, tail)
import Data.List
import Data.String
import Data.Int
import Data.Array
import Data.IORef
import Data.Function
import Data.Tuple
import Data.Maybe (fromJust)

import qualified Database.DIME.Server.Peers as Peers
import Database.DIME
import Database.DIME.Util
import Database.DIME.Transport
import Database.DIME.Server.State
import Database.DIME.Flow.Types
import Database.DIME.Memory.Block (ColumnType(..), ColumnValue)
import Database.DIME.Memory.Operation hiding (mapBlock)
import Database.DIME.DataServer.Command hiding (BlockInfo)
import Database.DIME.DataServer.Response

import Language.Flow.Module
import Language.Flow.Execution.Types
import Language.Flow.Execution.GMachine
import Language.Flow.Builtin

import System.Time

isTimeSeries :: GenericGData -> Bool
isTimeSeries = checkType (typeName (undefined :: TimeSeries))

asTimeSeries :: GenericGData -> TimeSeries
asTimeSeries = checkCoerce (typeName (undefined :: TimeSeries))

fetchTimeSeries :: TimeSeriesCollection -> ColumnName -> GMachine TimeSeries
fetchTimeSeries tsc columnName = do
  userState <- (getUserState :: GMachine QueryState)
  let key = queryKey userState
      ctxt = queryZMQContext userState
      serverName = queryServerName userState

      columns = tscColumns tsc

  columnType <- case lookup columnName columns of
    Nothing -> throwError $ "Cannot find column " ++ show columnName
    Just t -> return t

  let infoCmd = Peers.TimeSeriesColumnInfo key (tscName tsc) columnName
  liftIO $ sendRequest ctxt (Connect serverName) infoCmd $
      (\response -> case response of
              Peers.TimeSeriesColumnInfoResponse {..} ->
                  return $ TimeSeries {
                               tsTableName = tscName tsc,
                               tsColumnName = columnName,
                               tsTableId = tscTableId tsc,
                               tsColumnId = columnId,
                               tsDataType = columnType,
                               tsLength = tscLength tsc,
                               tsStartTime = tscStartTime tsc,
                               tsFrequency = tscFrequency tsc,
                               tsBlocks = M.fromList $ map (\(blockId, peerNames) -> (blockId, BlockInfo peerNames)) blocks,
                               tsBlockRanges = DIT.fromList $ map (\((s, e), o) -> ((fromIntegral s, fromIntegral e), o)) blockRanges,
                               tsRowMappings = DIT.fromList $ map (\((s, e), o) -> ((fromIntegral s, fromIntegral e), o)) rowMappings}
              Peers.ObjectNotFound -> let TimeSeriesName tsName = tscName tsc
                                          ColumnName cName = columnName
                                      in error $ "Could not find column: " ++ unpack cName ++ " of " ++ unpack tsName
              otherwise -> error $ "Invalid response from " ++ serverName)

instance GData TimeSeries where
    typeName _ = fromString "TimeSeries"
    constr _ = error "Cannot construct instances of TimeSeries from code"
    constrArgs _ = array (0, -1) []

    getField (unpack -> "name") ts = let ColumnName cName = tsColumnName ts
                                     in returnGeneric $ StringConstant cName
    getField (unpack -> "columnId") ts = returnGeneric $ IntConstant $ fromIntegral $ tsColumnId ts
    getField (unpack -> "columnType") ts = returnGeneric $ StringConstant $ fromString $
                                           case tsDataType ts of
                                             IntColumn -> "Int"
                                             StringColumn -> "String"
                                             DoubleColumn -> "Double"
    getField f _ = throwError $ "Invalid field for TimeSeries: " ++ unpack f

    supportsGeneric _ "add" = True
    supportsGeneric _ _ = False

    runGeneric _ "add" [x, y] = do
                            newTS <- addTS (asTimeSeries x) (asTimeSeries y)
                            returnPureGeneric newTS

allocColumn :: GMachine ColumnID
allocColumn = return (ColumnID 5)

getResultTable :: GMachine TableID
getResultTable = return (TableID 10)

conformTimeSeriesRow :: Context IO -> [((RowID, RowID), BlockRowID)] -> TimeSeries -> IO TimeSeries
conformTimeSeriesRow ctxt tsToBlockRowMappings ts
    | tsToBlockRowMappings == (DIT.assocs $ tsRowMappings ts) = return ts
    | otherwise = fail "Haven't yet implemented full row-level conformation"

conformTimeSeriesBlocks :: Context IO -> [((BlockRowID, BlockRowID), BlockID)] -> TimeSeries -> IO TimeSeries
conformTimeSeriesBlocks ctxt rowToIdMappings ts
    | rowToIdMappings == (DIT.assocs $ tsBlockRanges ts) = return ts
    | otherwise = fail "Haven't yet implemented full block-level conformation"

mapBlock :: Context IO -> MapOperation -> [TimeSeries] -> BlockSpec -> IO (ColumnType, BlockInfo)
mapBlock ctxt op timeSeriess resultBlock =
  do
    let BlockSpec _ _ blockId = resultBlock
        blockInfos = map (fromJust . (M.lookup blockId) . tsBlocks) timeSeriess

        -- get most popular peer
        allPeers = concatMap (\(BlockInfo peers) -> peers) blockInfos
        peerCounts = ((map (head &&& length)) . group . sort) allPeers
        mostPopularPeer = fst $ maximumBy (compare `on` snd) peerCounts
        PeerName mostPopularPeerTxt = mostPopularPeer

        blockSpecs = map (\ts -> BlockSpec (tsTableId ts) (tsColumnId ts) blockId) timeSeriess

    forM (zip timeSeriess blockInfos) $
         \(timeSeries, BlockInfo peers) ->
             when (not $ mostPopularPeer `elem` peers) $ do
               let blockSpec = BlockSpec (tsTableId timeSeries) (tsColumnId timeSeries) blockId
                   PeerName source = head peers
               sendRequest ctxt (Connect mostPopularPeerTxt)
                           (TransferBlock blockSpec blockSpec source) $
                           \response ->
                               case response of
                                 Ok -> return ()
                                 e -> fail $ "Inappropriate reply for block transfer: " ++ show e

    sendRequest ctxt (Connect mostPopularPeerTxt)
                (Map op blockSpecs resultBlock) $
                \response ->
                    case response of
                      MapResponse resultType -> return (resultType, BlockInfo [mostPopularPeer])
                      Fail e -> fail $ "Map failed: " ++ show e
                      e -> fail $ "Inappropriate reply to map: " ++ show e

mapTS :: MapOperation -> [TimeSeries] -> GMachine TimeSeries
mapTS op timeSeriess = do
  timeSeriess <- alignTSs timeSeriess
  let finalRowMappings = alignBlocks timeSeriess
      tsToBlockRowMappings = tail $
                             scanl (\((oldRowS, oldRowE), oldBlockS) (rowS, rowE) ->
                                        let blockS = oldBlockS + (fromIntegral $ oldRowE - oldRowS)
                                        in ((rowS, rowE), blockS))
                             ((RowID 0, RowID 0), BlockRowID 0) finalRowMappings

      finalLength = fromIntegral $ sum $ map (\((s, e), _) -> e - s) tsToBlockRowMappings
      ((firstRowIndex, _), _) = head tsToBlockRowMappings
      TOD startTimeS startTimePS = tsStartTime $ head timeSeriess
      finalStartTime = TOD ((fromIntegral $ fromIntegral firstRowIndex * tsFrequency (head timeSeriess)) + startTimeS) startTimePS

  ctxt <- getZMQContext

  timeSeriess <- liftIO $ mapP (conformTimeSeriesRow ctxt tsToBlockRowMappings) timeSeriess

  -- At this point, the time series we have are in row-level alignment
  -- this means that all blockrowids correspond, but the blocks may not correspond fully

  let masterBlockTemplate =
          let masterBlockTemplateTS = minimumBy (compare `on` (DIT.size . tsBlockRanges)) timeSeriess
          in DIT.assocs $ tsBlockRanges masterBlockTemplateTS

      blockIds = map snd masterBlockTemplate

  timeSeriess <- liftIO $ mapP (conformTimeSeriesBlocks ctxt masterBlockTemplate) timeSeriess

  tableId <- getResultTable
  columnId <- allocColumn

  -- At this point, the time series are in complete alignment, down to the blockid level
  typesAndBlocks <- liftIO $ mapP ((mapBlock ctxt op timeSeriess) . (BlockSpec tableId columnId)) blockIds

  let (types, blockInfos) = unzip typesAndBlocks
      blocks = zip blockIds blockInfos
      typesCheck = all (== (head types)) types
  when (not typesCheck) $ fail "time series map operation returned different types"

  return TimeSeries {
               tsTableName = fromString "",
               tsColumnName = fromString "",
               tsTableId = tableId,
               tsColumnId = columnId,
               tsDataType = head types,
               tsLength = finalLength,
               tsStartTime = finalStartTime,
               tsFrequency = tsFrequency $ head timeSeriess,
               tsBlocks = M.fromList blocks,
               tsBlockRanges = DIT.fromList masterBlockTemplate,
               tsRowMappings = DIT.fromList tsToBlockRowMappings
             }


addTS :: TimeSeries -> TimeSeries -> GMachine TimeSeries
addTS x y = mapTS Sum [x, y]

alignBlocks :: [TimeSeries] -> [(RowID, RowID)]
alignBlocks [] = []
alignBlocks [x] = DIT.keys $ tsRowMappings x
alignBlocks [x, y] =
    let taggedExtents = calcBlockAlignments (DIT.assocs $ tsRowMappings x) (DIT.assocs $ tsRowMappings y)
    in map (\(x, _, _) -> x) taggedExtents
alignBlocks (x:y:timeSeriess) =
    let tag = map (\x -> (x, ()))
        untag = map (\(x, _, _) -> x)
        startExtents = alignBlocks [x, y]
        getRowMappings ts = tag $ DIT.keys $ tsRowMappings ts
    in foldl (\x y -> untag $
                      calcBlockAlignments
                        (tag x)
                        (getRowMappings y))
       startExtents timeSeriess

forceTimeSeries :: TimeSeries -> GMachine ()
forceTimeSeries ts = do
  ctxt <- liftM queryZMQContext getUserState

  liftIO $ forM_ (M.assocs $ tsBlocks ts) $
       \(blkId, BlockInfo peers) ->
           let forceCmd = ForceComputation (BlockSpec (tsTableId ts) (tsColumnId ts) blkId)
               PeerName peer = head peers
           in sendRequest ctxt (Connect peer) forceCmd $
              \response -> case response of
                             Ok -> return ()
                             x -> fail $ "Bad response returned for ForceComputation: " ++ show x

readTimeSeries :: TimeSeries -> GMachine [(ClockTime, ColumnValue)]
readTimeSeries ts = do
  ctxt <- liftM queryZMQContext getUserState

  let blocksToRanges = M.fromList $ map swap $ DIT.assocs (tsBlockRanges ts)
      blockRanges = map (\((s, e), s') -> (s', s' + (fromIntegral $ e - s))) $ DIT.assocs (tsRowMappings ts)
      timeAnnotations = Data.List.concatMap (\(s, e) -> enumFromTo s (pred e)) blockRanges

      allBlocks = M.assocs $ tsBlocks ts

  blockData' <- forM allBlocks $
       \(blockId, BlockInfo peers) -> do
            let Just bounds = M.lookup blockId blocksToRanges
                fetchRowsCmd = FetchRows (tsTableId ts) [bounds] [tsColumnId ts]
                PeerName peerName = head peers
            liftIO $ sendRequest ctxt (Connect peerName) fetchRowsCmd $
                       \response -> case response of
                                      FetchRowsResponse results -> return $ map head results
                                      x -> fail $ "Could not successfully get block " ++ show blockId ++ " from " ++ show peers ++ ": " ++ show x

  let allBlockData = concat blockData'

      timedData = zip timeAnnotations allBlockData

      frequency = fromIntegral $ tsFrequency ts
      TOD startSeconds startPicoseconds = tsStartTime ts

      rowIdToTime (BlockRowID x) = TOD (startSeconds + frequency * fromIntegral x) startPicoseconds

      timedData' = map (\(row, val) -> (rowIdToTime row, val)) timedData

  return timedData'

alignTSs :: [TimeSeries] -> GMachine [TimeSeries]
alignTSs (x:xs)
    | all ((withinBounds (tsFrequency x) (tsStartTime x)) . tsStartTime) xs && all ((== (tsFrequency x)) . tsFrequency) xs = return $ x:xs
    | otherwise = fail "Alignment not yet implemented..."
 where
   withinBounds frequency time1 time2 = (time1 `absDiffClockTimesInSeconds` time2) < (fromIntegral frequency)

   absDiffClockTimesInSeconds :: ClockTime -> ClockTime -> Double
   absDiffClockTimesInSeconds t1@(TOD time1s time1ps) t2@(TOD time2s time2ps) =
       if (time1s, time1ps) <= (time2s, time2ps) then
           let secs = time2s - time1s
               psecsDiff = time1ps - time2ps
           in if psecsDiff < 0 then (fromIntegral $ secs - 1) + (fromIntegral (-psecsDiff) :: Double) / 1000000000000.0
              else (fromIntegral $ secs) + (fromIntegral psecsDiff :: Double) / 1000000000000.0
       else absDiffClockTimesInSeconds t2 t1

    -- -- | returns a pair, (ts1', ts2'), containing copies of t1 and t2, aligned with one another
    -- align :: TimeSeries -> TimeSeries -> DIME (TimeSeries, TimeSeries)
    -- align ts1 ts2 = do
    --   (ts1''', ts2''') <- alignStartTimes ts1 ts2 -- align time series start times
    --   (ts1'', ts2'') <- alignFrequency ts1''' ts2''' -- align time series frequencies
    --   (ts1', ts2') <- alignEndTimes ts1'' ts2'' -- align time series end times
    --   return $ (ts1', ts2')

    -- alignStartTimes :: TimeSeries -> TimeSeries -> DIME (TimeSeries, TimeSeries)
    -- alignStartTimes ts1 ts2 = do
    --   let ts1StartTime = tsStartTime ts1
    --       ts2StartTime = tsStartTime ts2

    --       ts1Smaller = ts1StartTime < ts2StartTime

    --       (smallerTs, largerTs) = if ts1Smaller then (ts1, ts2) else (ts2, ts1)

    --       samplesToDrop = floor $ (tsStartTime largerTs) - (tsStartTime smallerTs)

    --   smallerTs' <- dropSamples smallerTs samplesToDrop

    --   let nudgeFactor = (tsStartTime largerTs - tsStartTime smallerTs') / (tsDataFrequency smallerTs')

    --   smallerTs'' <- nudgeSamples Lerp smallerTs' nudgeFactor

    --   if ts1Smaller then
    --       (smallerTs'', largerTs   )
    --   else
    --       (largerTs   , smallerTs'')

    -- alignEndTimes :: TimeSeries -> TimeSeries -> DIME (TimeSeries, TimeSeries) -- Assumes time series start at the same time, and are frequency-aligned
    -- alignEndTimes ts1 ts2 = do
    --   if (tsEndTime ts1 < tsEndTime ts2) then do
    --       ts1' <- dropSamplesAtEnd (tsEndTime ts2 - tsEndTime ts1) ts1
    --       return (ts1', ts2)
    --   else
    --       ts2' <- dropSamplesAtEnd (tsEndTime ts1 - tsEndTime ts2) ts2
    --       return (ts1, ts2')

    -- alignFrequency :: blah
    -- alignFrequency = crunch
