{-# LANGUAGE TupleSections, ViewPatterns, RecordWildCards #-}
module Database.DIME.Flow.TimeSeries where

import Control.Monad.Trans
import Control.Monad

import qualified Data.Map as M
import qualified Data.Tree.DisjointIntervalTree as DIT
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Binary as Bin
import qualified Data.Tuple as Tuple
import Data.Text as T hiding (map, intercalate, all, head, foldl,
                              length, zip, splitAt, zipWith, concat,
                              last, groupBy)
import Data.List
import Data.String
import Data.Int
import Data.Array
import Data.IORef
import Data.Function

import qualified Database.DIME.Server.Peers as Peers
import Database.DIME
import Database.DIME.Transport
import Database.DIME.Server.State
import Database.DIME.Flow.Types
import Database.DIME.Memory.Block (ColumnType(..), ColumnValue)
import Database.DIME.Memory.Operation
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

allocNewColumn :: GMachine ColumnID
allocNewColumn = return (ColumnID 5)

getResultTable :: GMachine TableID
getResultTable = return (TableID 10)

mapTS :: MapOperation -> [TimeSeries] -> GMachine TimeSeries
mapTS op timeSeriess = do
  timeSeriess <- liftIO $ alignTSs timeSeriess
  let (masterTimeSeriesI, _) = minimumBy (compare `on` (M.size . tsBlocks . snd)) (zip [0..] timeSeriess) -- get timeseries with least number of blocks...
      timeSeries'' = let (init, _:tail) = splitAt masterTimeSeriesI timeSeriess
                     in init ++ tail -- splice out the master time series
      columnTypes = map tsDataType timeSeries''
      alignment = alignBlocks timeSeriess

      tsKey ts = (tsTableId ts, tsColumnId ts)
      timeSeriesMap = M.fromList $ zip (map tsKey timeSeriess) timeSeriess

      lookupPeerName (BlockSpec tableId columnId blockId) =
          let Just timeSeries = M.lookup (tableId, columnId) timeSeriesMap
              Just (BlockInfo peers) = M.lookup blockId $ tsBlocks timeSeries
              PeerName name = head peers
          in name

      transfer s bounds toBlockSpec fromBlockSpec fromPeer =
          let transferCmd = TransferBlock toBlockSpec fromBlockSpec fromPeer
          in sendOneRequest s transferCmd $
             (\(response :: Response) ->
                  when (isFailure response) $ fail ("Bad response from : " ++ show fromPeer ++ ": " ++ show response))

  ctxt <- getZMQContext

  -- Before we can make any requests, we need to know what blocks the result should go in, and also what intermediate blocks will have to be made
  resultColumn <- allocNewColumn
  resultTable <- getResultTable
  let (resultBlocks, newBlocks) = splitAt (length alignment) $ map (\blockId -> BlockSpec resultTable resultColumn blockId) [BlockID 1..]
      resultBlockIds = map (\(BlockSpec _ _ x) -> x) resultBlocks

  newBlocksRef <- liftIO $ newIORef newBlocks

  let groupedAlignments = groupBy ((==) `on` ((!! masterTimeSeriesI) . snd)) alignment
      alignment' = map (\alignmentGroup -> let (bounds, blocks) = head alignmentGroup
                                               masterBlock = blocks !! masterTimeSeriesI
                                               alignmentGroup' = map (\(bounds, xs) -> let (init, _:tail) = splitAt masterTimeSeriesI xs
                                                                                       in (bounds, init ++ tail)) alignmentGroup
                                           in (masterBlock, alignmentGroup')) groupedAlignments

      newBlock = do
        (blockId:newBlocks) <- liftIO $ readIORef newBlocksRef
        writeIORef newBlocksRef newBlocks
        return blockId

  let justOneBlock alignment = let oneBlockEach = all (\(_, blocks) -> length blocks == 1) alignment
                                   blocksMatch = let headBlock = head $ snd $ head alignment
                                                 in all (\(_, blocks) -> head blocks == headBlock) alignment
                               in oneBlockEach && blocksMatch

      addBounds bounds = (fst $ head bounds, snd $ last bounds)

  alignment'' <- liftIO $ forM alignment' $
    (\(masterBlock, alignmentGroup) ->
         case alignmentGroup of
           _
             | justOneBlock alignmentGroup -> return (masterBlock, [head $ snd $ head $ alignmentGroup], [(addBounds $ map fst alignmentGroup, [head $ snd $ head alignmentGroup])]) -- copying a single block...
           (_, blocks):_ -> do -- more than one element
             blockIds <- replicateM (length blocks) newBlock
             return (masterBlock, blockIds, alignmentGroup))

  -- copy all necessary blocks
  forM_ alignment'' $
    (\(masterBlockSpec, destBlocks, alignment) ->
       liftIO $ safelyWithSocket ctxt Req (Connect $ lookupPeerName masterBlockSpec) $
       (\s -> do
         forM_ (zip columnTypes destBlocks) $
           (\(columnType, destBlock) ->
                let minRow = fst $ fst $ head alignment
                    maxRow = snd $ fst $ last alignment
                    BlockSpec tblId _ _ = destBlock
                in when (tblId == resultTable) $
                   sendOneRequest s (NewBlock destBlock minRow maxRow columnType) $
                   (\(response :: Response) ->
                        when (isFailure response) $ fail ("Bad repsonse: " ++ show response)))
         forM_ alignment $
           (\(bounds, blockSpecs) ->
                forM_ (zip destBlocks blockSpecs) $
                  (\(destBlockSpec, blockSpec) ->
                     let peerName = lookupPeerName blockSpec
                     in when (destBlockSpec /= blockSpec) $ do
                       transfer s bounds blockSpec destBlockSpec peerName))))

  -- now issue map operations...
  retTypes <- forM (zip resultBlocks alignment'') $
    (\(resultBlock, (masterBlockSpec, destBlock, ((minRow, _),_):_)) ->
         let (argsInit, argsTail) = splitAt masterTimeSeriesI destBlock
             blockArgs = argsInit ++ [masterBlockSpec] ++ argsTail
             mapCmd = Map op blockArgs resultBlock
         in liftIO $ sendRequest ctxt (Connect $ lookupPeerName masterBlockSpec) mapCmd $
                (\(response :: Response) ->
                     case response of
                       MapResponse retType -> return retType
                       Fail e -> fail $ "Map failed: " ++ show e
                       InconsistentTypes -> fail $ "Map failed: mismatched types"
                       BlockDoesNotExist -> fail $ "Internal map error: block does not exist on master"
                       _ -> fail ("Bad response for map " ++ show mapCmd ++ ": " ++ show response)))

  let typesCheck = all (== (head retTypes)) retTypes
      blockMap = M.fromList $ zipWith (\(blockSpec, _) destBlockId -> (destBlockId, BlockInfo [PeerName $ lookupPeerName blockSpec])) alignment' resultBlockIds
      -- alignment'' isn't used because we merge block bounds together
      rowMappings = DIT.fromList $ concat $ zipWith (\(_, mappings) destBlockId -> map (\(bound, _) -> (bound, destBlockId)) mappings) alignment' resultBlockIds
  unless typesCheck $ throwError "Map function returned different types"

  return $ TimeSeries {
               tsTableName = fromString "",
               tsColumnName = fromString "",
               tsTableId = resultTable,
               tsColumnId = resultColumn,
               tsDataType = head retTypes,
               tsLength = tsLength $ head timeSeriess,
               tsStartTime = tsStartTime $ head timeSeriess,
               tsFrequency = tsFrequency $ head timeSeriess,
               tsBlocks = blockMap,
               tsRowMappings = rowMappings}

addTS :: TimeSeries -> TimeSeries -> GMachine TimeSeries
addTS x y = mapTS Sum [x, y]

alignBlocks :: [TimeSeries] -> [((RowID, RowID), [BlockSpec])]
alignBlocks = alignBlocks'
    where
      alignBlocks' [] = []
      alignBlocks' [x] = tsAssocs x
      alignBlocks' [x,y] = mergeAlignmentResult $ calcBlockAlignments (tsAssocs x) (tsAssocs y)
      alignBlocks' (x:y:timeSeriess) =
          let assocss = map tsAssocs timeSeriess
          in foldl (\x y -> mergeAlignmentResult $ calcBlockAlignments x y) (alignBlocks [x, y]) assocss
      tsAssocs ts =
        let basicAssocs = DIT.assocs $ tsRowMappings ts
            tableId = tsTableId ts
            columnId = tsColumnId ts
            blockSpecAssocs = map (\(bounds, x) -> (bounds, BlockSpec tableId columnId x)) basicAssocs
        in liftAssocs blockSpecAssocs
      liftAssocs = map (\(bounds, a) -> (bounds, [a]))
      mergeAlignmentResult = map (\(bounds, x, y) -> (bounds, x ++ y))

readTimeSeries :: TimeSeries -> GMachine [(ClockTime, ColumnValue)]
readTimeSeries ts = do
  let assocs = DIT.assocs $ tsRowMappings ts

      lookupBlockData blockId = let Just blockInfo = M.lookup blockId $ tsBlocks ts
                                in blockInfo

      -- assocsByBlock has the structure [(BlockID, [Bounds])]
      assocsByBlock = map (\blockAssocs -> (snd $ head blockAssocs, map fst blockAssocs)) $ groupBy ((==) `on` snd) assocs

      assocsWithBlockData = map (\(blockId, bounds) -> (blockId, lookupBlockData blockId, bounds)) assocsByBlock

      expandBounds = Data.List.concatMap (uncurry enumFromTo)

  ctxt <- liftM queryZMQContext getUserState

  blockDatas <- forM assocsWithBlockData $
       \(blockId, BlockInfo peers, bounds) -> do
           let fetchRowsCmd = FetchRows (tsTableId ts) bounds [tsColumnId ts]
               PeerName peerName = head peers
           blockData <- liftIO $ sendRequest ctxt (Connect peerName) fetchRowsCmd $
                        \response -> case response of
                                       FetchRowsResponse results -> return $ (map head results)
                                       x -> fail $ "Could not successfully get block " ++ show blockId ++ " from " ++ show peers ++ ": " ++ show x
           return $ zip (expandBounds bounds) blockData

  let allBlockData = concat blockDatas

      startTime = tsStartTime ts
      frequency = fromIntegral $ tsFrequency ts

      rowIdToTime (RowID x) = let TOD seconds picoseconds = startTime
                              in TOD (seconds + frequency * fromIntegral x) picoseconds

      timedData = map (\(row, val) -> (rowIdToTime row, val)) allBlockData
  return timedData


alignTSs :: [TimeSeries] -> IO [TimeSeries]
alignTSs (x:xs)
    | all ((withinBounds (tsFrequency x) (tsStartTime x)) . tsStartTime) xs && all ((== (tsFrequency x)) . tsFrequency) xs = return $ x:xs
    | otherwise = error "Alignment not yet implemented..."
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

    -- mapTS :: MapOperation -> [TimeSeries] -> IO TimeSeries
    -- mapTS op tss = do
    --   when (tsNotAligned tss) $ fail "Cannot map unaligned time series"
    --   tss' <- mapM forceTS tss -- Make sure we have the full version of each timeseries
    --   let tssBlocks = map (\ts -> map (tsTableId ts, tsColumnId ts,) $ tsBlocks ts) tss'
    --       tssBlocks' = L.transpose tssBlocks -- Now we have a list of lists. Each element is a list of corresponding blocks in each time series
    --   tssBlocks'' <- mapM migrateBlocks tssBlocks' -- Migrate blocks so that all blocks for each submap are on the same server

    --   -- Allocate a new table ID from the main server
    --   tableId <- allocTableId

    --   tssBlocks''' <- mapM (issueMap tableId (ColumnID 0) op) tssBlocks''
    --   return $ mkAbsTimeSeries tableId (ColumnID 0) (tsAlignment $ head tss) tssBlocks''' -- TODO get the DIT down as well!

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
