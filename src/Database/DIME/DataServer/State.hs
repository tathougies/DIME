{-# LANGUAGE ExistentialQuantification, RankNTypes, BangPatterns, RecordWildCards #-}
module Database.DIME.DataServer.State
    ( DataServerState, GenericBlock(..),
      GenericBlockIO(..),
      mkEmptyServerState,
      emptyBlockFromType, insertBlock, hasBlock,
      mapServerBlock, collapseServerBlock, forceCompute,
      hasTable, hasColumn, hasRow,
      deleteBlock, getBlockInfo, withGenericBlock,
      modifyGenericBlock,
      withGenericBlockIO,

      getBlockCount,

      fetchColumnForRow, updateRows, establishRowMapping,

      dumpState
    )
    where
import qualified Codec.Compression.GZip as GZip

import qualified Control.Exception as E
import Control.Concurrent.MVar
import Control.Monad
import Control.Applicative

import qualified Database.DIME.Memory.Block as B
import Database.DIME.Memory.Block ((#!))
import Database.DIME.Memory.BlockInfo as BI
import Database.DIME.Memory.Operation.Mappable as BM
import Database.DIME.Memory.Operation.Collapsible as BC
import Database.DIME.Memory.Operation.Int ()
import Database.DIME.Memory.Operation.Double ()
import Database.DIME.Memory.Operation.String ()
import Database.DIME.DataServer.Command
import Database.DIME

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Tree.DisjointIntervalTree as DIT
import qualified Data.List as L
import qualified Data.Binary as Bin
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Data.List.Collect
import Data.IORef
import Data.Maybe
import Data.Typeable
import Data.Function
import Data.Char

import Debug.Trace

import System.IO.Unsafe
import System.Directory
import System.FilePath

import Text.JSON as JSON

import Unsafe.Coerce

-- | data type to hold any generic block. There are convenience methods to cast (safely) to specific block types
data GenericBlock = forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => GenericBlock !BlockRowID !(B.Block a)
data GenericBlockIO = forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => GenericBlockIO !BlockRowID (B.BlockIO a)

instance Show GenericBlock where
    show (GenericBlock r b) = "GenericBlock " ++ (show r) ++ " " ++ (show b)

-- | The data structure containing the server state
data DataServerState = DataServerState {
      -- | Maintains a mapping between table IDs and all columns (that we keep track of) that belong to that table
      getTables :: M.Map TableID (S.Set ColumnID),

      -- | Maintains a mapping between block identifiers and their blocks
      getBlocks :: M.Map BlockSpec (MVar GenericBlockIO),

      -- | Maps between a column identifier and a 'DisjointIntervalTree', which keeps track of which rows go in which blocks
      getRowMappings :: M.Map (TableID, ColumnID) (MVar (DIT.DisjointIntervalTree BlockRowID BlockID)),

      -- | A list of blocks modified since the last dump
      getModifiedBlocks :: S.Set BlockSpec,

      -- | The directory in which to store chunks and mapping file
      getChunkDir :: FilePath
    }

blockFileName :: FilePath -> BlockSpec -> FilePath
blockFileName chunkDir (BlockSpec tableId columnId blockId) = chunkDir </> "blk_" ++ showAsI tableId ++ "_" ++ showAsI columnId ++ "_" ++ showAsI blockId

columnFileName :: FilePath -> (TableID, ColumnID) -> FilePath
columnFileName chunkDir (tableId, columnId) = chunkDir </> "col_" ++ showAsI tableId ++ "_" ++ showAsI columnId

tableFileName :: FilePath -> FilePath
tableFileName chunkDir = chunkDir </> "tables"

showAsI :: Integral a => a -> String
showAsI x = show (fromIntegral x :: Int)

mkEmptyServerState :: FilePath -> IO (MVar DataServerState)
mkEmptyServerState chunkDir = do
  let emptyState = DataServerState M.empty M.empty M.empty S.empty chunkDir
  -- check if files exist
  stateExists <- doesFileExist $ tableFileName chunkDir
  finalState <- if stateExists then  restoreStateFromChunks emptyState
                else return emptyState
  newMVar finalState

restoreStateFromChunks :: DataServerState -> IO DataServerState
restoreStateFromChunks state@DataServerState{ getChunkDir = chunkDir} = do
  -- parse tables json
  tablesJSONData <- readFile $ tableFileName chunkDir
  let allBlocks = case (JSON.decode tablesJSONData :: Result [BlockSpec]) of
                    Error e -> fail $ "Could not read tables data: " ++ e
                    Ok blocks -> blocks

      allColumns = S.toList $ S.fromList $ map (\(BlockSpec tableId columnId _) -> (tableId, columnId)) allBlocks -- get all unique columns

  tablesAndRowMappings <-
    forM allColumns $
       (\columnSpec -> do
          columnData <- readFile $ columnFileName chunkDir columnSpec
          let (tableId, columnId, rowMappings) = case JSON.decode columnData of
                  Ok (JSObject columnDict) -> parseColumn columnDict
                  Ok x -> error $ "Couldn't read column " ++ show columnSpec ++ ": bad type"
                  Error e -> error $ "Couldn't read column " ++ show columnSpec ++ ": " ++ e
              columnsForTable = maybe S.empty id $ M.lookup tableId $ getTables state
              columnsForTable' = S.insert columnId columnsForTable
          putStrLn ("mapping: " ++ show (((tableId, columnsForTable'), ((tableId, columnId), rowMappings))))

          rowMappingsVar <- newMVar rowMappings
          return ((tableId, columnsForTable'), ((tableId, columnId), rowMappingsVar)))
  let (tableAssocs, rowMappingAssocs) = unzip tablesAndRowMappings
      tables = M.fromList tableAssocs
      rowMappings = M.fromList rowMappingAssocs

  blockAssocs <-
    forM allBlocks $
       (\blockSpec -> do
          blockData <- readFile $ blockFileName chunkDir blockSpec

          blk <- case JSON.decode blockData of
                   Ok (JSObject blockDict) -> return (parseBlock blockDict)
                   Ok x -> fail ("Couldn't read block " ++ show blockSpec ++ ": bad type")
                   Error e -> fail ("Couldn't read block " ++ show blockSpec ++ ": " ++ e)

          warmBlock <- genericThaw blk
          blockVar <- newMVar warmBlock
          return (blockSpec, blockVar))

  putStrLn ("going to show tables")
  putStrLn ("tables: " ++ show tableAssocs)

  return state {
    getTables = M.fromList tableAssocs,
    getRowMappings = M.fromList rowMappingAssocs,
    getBlocks = M.fromList blockAssocs }
 where
    readRowMappings :: JSValue -> [((BlockRowID, BlockRowID), BlockID)]
    readRowMappings (JSArray x) = map readRowMapping x
    readRowMappings _ = error "Bad JSON data type for row mappings"

    readRowMapping :: JSValue -> ((BlockRowID, BlockRowID), BlockID)
    readRowMapping (JSObject o) = let mappingData = fromJSObject o
                                      l fieldName = lookup fieldName mappingData
                                  in case (l "range", l "blockId") of
                                        (Just rangeD, Just blockIdD) ->
                                            case (readRange rangeD, readJSON blockIdD) of
                                                (range, Ok blockId) -> (range, blockId)
                                                _ -> error "Bad data types for row mapping"
                                        _ -> error "Bad object structure for row mapping"
    readRowMapping _ = error "Bad JSON data type for row mapping"

    readRange :: JSValue -> (BlockRowID, BlockRowID)
    readRange (JSObject o) = let rangeData = fromJSObject o
                                 l fieldName = lookup fieldName rangeData
                             in case (l "start", l "end") of
                                  (Just startD, Just endD) ->
                                      case (readJSON startD, readJSON endD) of
                                        (Ok start, Ok end) -> (start, end)
                                        _ -> error "Bad data types for range"
                                  _ -> error "Bad object structure for range"
    readRange _ = error "Bad JSON data type for range"

    parseColumn :: JSObject JSValue -> (TableID, ColumnID, DIT.DisjointIntervalTree BlockRowID BlockID)
    parseColumn columnJson = let columnData = fromJSObject columnJson
                                 l fieldName = lookup fieldName columnData
                             in
                               case (l "tableId", l "columnId", l "rowMappings") of
                                 (Just tableIdD, Just columnIdD, Just rowMappingsD) ->
                                     case (readJSON tableIdD, readJSON columnIdD, readRowMappings rowMappingsD) of
                                       (Ok tableId, Ok columnId, rowMappings) -> (tableId, columnId, DIT.fromList rowMappings)
                                       _ -> error "Bad data types for column"
                                 _ -> error "Bad object structure for column"

    parseBlock :: JSObject JSValue -> GenericBlock
    parseBlock blockJson = let blockData = fromJSObject blockJson
                               l fieldName = lookup fieldName blockData
                           in
                             case (l "startRow", l "data", l "type") of
                               (Just startRowD, Just dataD, Just typeD) ->
                                   case (readJSON startRowD, readJSON dataD, readJSON typeD) of
                                     (Ok startRow, Ok blkData, Ok blkType) ->
                                         readGenericBlock blkType blkData startRow
                                     _ -> error "Bad data types for block"
                               _ -> error "Bad object structure for block"


    readBlock :: B.BlockStorable s => String -> B.Block s
    readBlock blkData =
      let blkDataBs = BS.pack $ map (fromIntegral . ord) blkData
      in case B64.decode blkDataBs of
           Left e -> error e
           Right blkDataDecoded ->
               let blkDataList = Bin.decode $ GZip.decompress $ LBS.fromChunks [blkDataDecoded]
                   blkLength = L.length blkDataList
                   blkDataUpdate = zip [0..blkLength - 1] blkDataList
               in
                 B.update (B.resize blkLength B.empty) blkDataUpdate

    readGenericBlock :: B.ColumnType -> String -> BlockRowID -> GenericBlock
    readGenericBlock columnType blkData startRow =
        case columnType of
          B.IntColumn -> GenericBlock startRow $ (readBlock blkData :: B.Block Int)
          B.DoubleColumn -> GenericBlock startRow $ (readBlock blkData :: B.Block Double)
          B.StringColumn -> GenericBlock startRow $ (readBlock blkData :: B.Block String)

getBlockCount :: DataServerState -> Int
getBlockCount st = M.size $ getBlocks st

emptyBlockFromType :: BlockRowID -> Int -> B.ColumnType -> IO GenericBlockIO
emptyBlockFromType firstRow size columnType = case columnType of
  B.IntColumn -> GenericBlockIO firstRow <$> (B.newM size :: IO (B.BlockIO Int))
  B.StringColumn -> GenericBlockIO firstRow <$> (B.newM size :: IO (B.BlockIO String))
  B.DoubleColumn -> GenericBlockIO firstRow <$> (B.newM size :: IO (B.BlockIO Double))

-- | Insert the block but also insert the column into the table
insertBlock :: BlockSpec -> GenericBlockIO -> DataServerState -> IO DataServerState
insertBlock blockSpec@(BlockSpec tableId columnId _) block s = do
  blockVar <- newMVar block
  let insertColumn Nothing = Just $ S.singleton columnId
      insertColumn (Just columns) = Just $ S.insert columnId columns
  return s { getBlocks = M.insert blockSpec blockVar $ getBlocks s,
             getTables = M.alter insertColumn tableId $ getTables s,
             getModifiedBlocks = S.insert blockSpec $ getModifiedBlocks s }

hasBlock :: BlockSpec -> DataServerState -> Bool
hasBlock blockSpec DataServerState {getBlocks = blocks} = M.member blockSpec blocks

hasTable :: TableID -> DataServerState -> Bool
hasTable tableId DataServerState { getTables = tables } =
    case M.lookup tableId tables of
      Nothing -> False
      Just _ -> True

hasColumn :: TableID -> ColumnID -> DataServerState -> Bool
hasColumn tableId columnId DataServerState { getTables = tables } =
    let columnSetResult = M.lookup tableId tables
    in case columnSetResult of
         Nothing -> error "Table not found"
         Just columnSet -> S.member columnId columnSet

hasRow :: TableID -> ColumnID -> BlockRowID -> DataServerState -> IO Bool
hasRow tableId columnId rowId DataServerState { getRowMappings = rowMappings} =
    let rowMappingResult = M.lookup (tableId, columnId) rowMappings
    in case rowMappingResult of
         Nothing -> fail "Cannot find table and column pair"
         Just rowMappingVar -> do
           withMVar rowMappingVar $ \rowMapping ->
             return $! isJust (DIT.lookup rowId rowMapping)

deleteBlock :: BlockSpec -> DataServerState -> IO DataServerState
deleteBlock blockSpec@(BlockSpec tableId columnId _) s = do
    let s' = s { getBlocks = M.delete blockSpec $ getBlocks s,
                 getModifiedBlocks = S.insert blockSpec $ getModifiedBlocks s }
    blockInfo <- getBlockInfo blockSpec s
    case M.lookup (tableId, columnId) (getRowMappings s) of
      Nothing -> return s'
      Just rowMappingVar -> do
        modifyMVar_ rowMappingVar $ return . DIT.delete (firstRow blockInfo, lastRow blockInfo)
        return s'

withAllMVars :: [MVar a] -> ([a] -> IO b) -> IO b
withAllMVars mvars action = doWithAllMVars mvars id action
  where doWithAllMVars [] a action = action (a [])
        doWithAllMVars (lockVar:locks) a action = do
          withMVar lockVar $ \lock ->
            doWithAllMVars locks (a . (lock:)) action

-- | The block id list should be sorted
withBlocks :: DataServerState -> [BlockSpec] -> ([GenericBlockIO] -> IO a) -> IO a
withBlocks state blocks action = withAllMVars (map getBlock blocks) action
  where getBlock blockSpec = fromJust . M.lookup blockSpec $ getBlocks state

withBlocksUnsorted :: DataServerState -> [BlockSpec] -> ([GenericBlockIO] -> IO a) -> IO a
withBlocksUnsorted state blocks action = do
  let sortedBlockSpecs = L.sort blocks
  withBlocks state sortedBlockSpecs $ \sortedBlocks ->
    let blockMap = M.fromAscList (zip sortedBlockSpecs sortedBlocks)
        unsortedBlocks = map (fromJust . flip M.lookup blockMap) blocks -- unsort according to the original order!
    in action unsortedBlocks

-- | Updates the rows in the given column with the given values. The rows must be sorted, and
-- | The columns must have the correct type or bad things will happen!
updateRows :: TableID -> ColumnID -> [BlockRowID] -> [B.ColumnValue] -> DataServerState -> IO DataServerState
updateRows tableId columnId rowIds values state = do
    let Just rowMappingsVar = M.lookup (tableId, columnId) $ getRowMappings state
    blockIds <- withMVar rowMappingsVar $ \rowMappings -> do
      let blockIdForRow rowId = fromJust $ DIT.lookup rowId rowMappings
      return (map blockIdForRow rowIds)
    let blocksIdsAndValues = zip blockIds (zip rowIds values)
        blockUpdateDescrs = collect blocksIdsAndValues -- Group rows from the same block together, returns them in sorted order

        updateBlock (blockId, block) idsAndValues = do
          let blockStartIndex = genericFirstRowIO block
              rowIdsAndValues = map (\(x, B.ColumnValue y) -> (fromIntegral $ x - blockStartIndex, unsafeCoerce y)) idsAndValues
              blockSpec = BlockSpec tableId columnId blockId
              typeCheck block = genericTypeOfIO block == (B.withColumnValue typeOf (snd . head $ idsAndValues))
          if typeCheck block
            then do
              withGenericBlockIO (\block -> mapM_ (uncurry (B.updateM block)) rowIdsAndValues) block
            else fail "Incorrect type supplied for block"
    putStrLn (show blockUpdateDescrs)
    -- We need to lock each block first
    withBlocks state (map (\(blockId,_) -> BlockSpec tableId columnId blockId) blocksIdsAndValues) $ \blocks ->
      let blockUpdateDescrs' = zip (zip (map fst blockUpdateDescrs) blocks) (map snd blockUpdateDescrs)
      in do
        mapM_ (uncurry updateBlock) blockUpdateDescrs'
    putStrLn ("After " ++ show blockUpdateDescrs)
    return state { getModifiedBlocks = S.fromList (map (\blockId -> BlockSpec tableId columnId blockId) blockIds) `S.union` getModifiedBlocks state}

fetchColumnForRow :: TableID -> ColumnID -> BlockRowID -> DataServerState -> IO B.ColumnValue
fetchColumnForRow tableId columnId rowId state = do
    let Just rowMappingVar = M.lookup (tableId, columnId) $ getRowMappings state
    withMVar rowMappingVar $ \rowMapping -> do
      let Just blockId = DIT.lookup rowId rowMapping
          Just blockVar = M.lookup (BlockSpec tableId columnId blockId) $ getBlocks state
      withMVar blockVar $ \block -> do
        let offsetIndex = rowId - (genericFirstRowIO block)
        withGenericBlockIO (\block' -> B.ColumnValue <$> B.indexM block' (fromIntegral offsetIndex)) block

getBlockInfo :: BlockSpec -> DataServerState -> IO BlockInfo
getBlockInfo blockSpec DataServerState {getBlocks = blocks} = do
    let Just blockVar = M.lookup blockSpec blocks
    withMVar blockVar $ \block -> do
      let firstRow = genericFirstRowIO block
      blockLength <- fromIntegral <$> genericLengthIO block
      return (BI.empty { firstRow = firstRow, lastRow = firstRow + blockLength - BlockRowID 1,
                         blockType = B.typeRepToColumnType $ genericTypeOfIO block })

forceCompute :: BlockSpec -> DataServerState -> IO ()
forceCompute blockSpec st =
  case M.lookup blockSpec (getBlocks st) of
    Nothing -> return ()
    Just blockVar -> withMVar blockVar $ withGenericBlockIO B.forceComputeM

mapServerBlock :: MapOperation -> [BlockSpec] -> BlockSpec -> DataServerState -> IO (Either String DataServerState)
mapServerBlock op [] _ state = return (Left "not enough arguments to map operation")
mapServerBlock op inputSpecs output@(BlockSpec tableId columnId blockId) state = do
  res <- withBlocksUnsorted state inputSpecs $ \blocks -> do
    let firstBlockType = genericTypeOfIO $ head blocks
        firstRow = genericFirstRowIO $ head blocks
        blocksTypeCheck = all ((== firstBlockType) . genericTypeOfIO) blocks
    if blocksTypeCheck
      then do
       frozenBlocks <- mapM genericFreeze blocks
       return (Just (frozenBlocks, firstBlockType, firstRow))
      else return Nothing
  case res of
    Nothing -> return (Left "Cannot map blocks of different types")
    Just (frozenBlocks, firstBlockType, firstRow) -> do
      let createBlock = createBlockFromType firstBlockType

          createBlockFromType t
            | t == typeOf (undefined :: Int) =
              let res = mapBlock op $ map (fromJust . genericCoerceInt) frozenBlocks
              in maybe Nothing (Just . GenericBlock firstRow) res
            | t == typeOf (undefined :: Double) =
              let res = mapBlock op $ map (fromJust . genericCoerceDouble) frozenBlocks
              in maybe Nothing (Just . GenericBlock firstRow) res
            | t == typeOf (undefined :: String) =
              let res = mapBlock op $ map (fromJust . genericCoerceString) frozenBlocks
              in maybe Nothing (Just . GenericBlock firstRow) res
      case createBlock of
        Nothing -> return (Left "Map failed")
        Just newBlock -> do
          newBlock' <- genericThaw newBlock
          state' <- insertBlock output newBlock' state
          length <- fromIntegral <$> genericLengthIO newBlock'
          Right <$> establishRowMapping (tableId, columnId) (firstRow, firstRow + length) blockId state

collapseServerBlock :: Command -> DataServerState -> IO (Either String DataServerState)
collapseServerBlock Collapse {..} state = do
    let Just genericInputBlockVar = M.lookup collapseBlockSpec $ getBlocks state
    inputBlock <- withMVar genericInputBlockVar genericFreeze
    let blockType = genericTypeOf inputBlock
        firstRow = genericFirstRow inputBlock
    usingGenericBlock inputBlock $ \typedBlock -> do
      let finalInputBlock = typedBlock `B.append` (B.fromList collapseBlockSuffix)
      case collapseBlock collapseOperation collapseLength finalInputBlock of
        Nothing -> return (Left "collapse failed")
        Just x -> do
          let resultBlock = GenericBlock firstRow x
              BlockSpec resultTableId resultColumnId resultBlockId = collapseResultBlock
          thawedBlock <- genericThaw resultBlock
          state' <- insertBlock collapseResultBlock thawedBlock state
          Right <$> establishRowMapping (resultTableId, resultColumnId) (firstRow, firstRow + BlockRowID(fromIntegral $ genericLength resultBlock)) resultBlockId state'

establishRowMapping :: (TableID, ColumnID) -> (BlockRowID, BlockRowID) -> BlockID -> DataServerState -> IO DataServerState
establishRowMapping blockKey bounds blockId st@(DataServerState {getRowMappings = rowMappings}) = do
    let updateRowMapping :: Maybe (DIT.DisjointIntervalTree BlockRowID BlockID) -> Maybe (DIT.DisjointIntervalTree BlockRowID BlockID)
        updateRowMapping value = let tree = maybe DIT.empty id value
                                 in Just $ DIT.insert bounds blockId tree
    case M.lookup blockKey rowMappings of
      Nothing -> do
        newDit <- newMVar (DIT.insert bounds blockId DIT.empty)
        return (st { getRowMappings = M.insert blockKey newDit (getRowMappings st) })
      Just rowMappingVar -> do
        modifyMVar_ rowMappingVar (return . DIT.insert bounds blockId)
        return st

genericLength :: GenericBlock -> Int
genericLength = withGenericBlock B.length

genericLengthIO :: GenericBlockIO -> IO Int
genericLengthIO = withGenericBlockIO B.lengthM

genericTypeOf :: GenericBlock -> TypeRep
genericTypeOf = withGenericBlock (\x -> typeOf $ x #! 0)

genericTypeOfIO :: GenericBlockIO -> TypeRep
genericTypeOfIO (GenericBlockIO _ b) =
  let rep = head $ typeRepArgs (typeOf (B.indexM b 0))
  in trace (show (typeOf (B.indexM b 0))) rep

genericFirstRow :: GenericBlock -> BlockRowID
genericFirstRow (GenericBlock r _) = r

genericFirstRowIO :: GenericBlockIO -> BlockRowID
genericFirstRowIO (GenericBlockIO r _) = r

withGenericBlock :: (forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => B.Block a -> b) -> GenericBlock -> b
withGenericBlock f (GenericBlock _ b) = f b

withGenericBlockIO :: (forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => B.BlockIO a -> IO b) -> GenericBlockIO -> IO b
withGenericBlockIO f (GenericBlockIO _ b) = f b

usingGenericBlock = flip withGenericBlock

genericThaw :: GenericBlock -> IO GenericBlockIO
genericThaw (GenericBlock l b) = do
  bIO <- B.thaw b
  return (GenericBlockIO l bIO)

genericFreeze :: GenericBlockIO -> IO GenericBlock
genericFreeze (GenericBlockIO l bIO) = do
  b <- B.freeze bIO
  return (GenericBlock l b)

genericCoerceInt :: GenericBlock -> Maybe (B.Block Int)
genericCoerceInt block
    | genericTypeOf block == typeOf (undefined :: Int) = Just $ withGenericBlock unsafeCoerce block
    | otherwise = Nothing

genericCoerceDouble :: GenericBlock -> Maybe (B.Block Double)
genericCoerceDouble block
    | genericTypeOf block == typeOf (undefined :: Double) = Just $ withGenericBlock unsafeCoerce block
    | otherwise = Nothing

genericCoerceString :: GenericBlock -> Maybe (B.Block String)
genericCoerceString block
    | genericTypeOf block == typeOf (undefined :: String) = Just $ withGenericBlock unsafeCoerce block
    | otherwise = Nothing

-- | Apply transformation functions on the fields of a generic block
--   Captures type variables, so it makes the type system happy
modifyGenericBlock :: (BlockRowID -> BlockRowID) -> (forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => B.Block a -> B.Block a) -> GenericBlock -> GenericBlock
modifyGenericBlock rowIndexF dataF (GenericBlock r b) = GenericBlock (rowIndexF r) (dataF b)

dumpState :: MVar DataServerState -> IO ()
dumpState stateVar = do
  (chunkDir, allBlocks, updatedBlocks, updatedColumns) <-
    E.bracket (takeMVar stateVar) (\state -> putMVar stateVar $ state {getModifiedBlocks = S.empty }) $
      \state -> do
        let modifiedBlocks = S.toList $ getModifiedBlocks state
            allModifiedColumns = L.sort $ map (\(BlockSpec tableId columnId _) -> (tableId, columnId)) modifiedBlocks
            modifiedColumns = L.nub allModifiedColumns
            columnVars = catMaybes (map (flip M.lookup (getRowMappings state)) modifiedColumns)
            updatedBlocks = map (\blockSpec ->
                              case M.lookup blockSpec (getBlocks state) of
                                Nothing -> error $ "Deleted " ++ show blockSpec ++ " not handled"
                                Just block -> (blockSpec, block)) modifiedBlocks
            allBlocks = M.keys (getBlocks state)
        updatedColumns <- withAllMVars columnVars $ \columns -> return (zip modifiedColumns (map DIT.assocs columns))
        return (getChunkDir state, allBlocks, updatedBlocks, updatedColumns)

  -- the row ranges of modified columns should be written out again

  -- TODO Before writing all these out, we should write a transaction log to the disk

  forM updatedBlocks $ -- write out updated block data
       (\(updatedBlockSpec@(BlockSpec tableId columnId blockId), updatedBlockVar) ->
           withMVar updatedBlockVar $ \updatedBlock -> do
             blockData <- withGenericBlockIO ((fmap Bin.encode) . B.toListM) updatedBlock
             let fileName = blockFileName chunkDir updatedBlockSpec
                 blockDataTxt = B64.encode $ BS.concat $ LBS.toChunks $ GZip.compress $ blockData
             updatedBlockLength <- genericLengthIO updatedBlock
             let endRow =  (fromIntegral $ genericFirstRowIO updatedBlock) + updatedBlockLength - 1
                 jsonData = showJSON $ toJSObject [
                              ("tableId", showJSON tableId),
                              ("columnId", showJSON columnId),
                              ("blockId", showJSON blockId),
                              ("startRow", showJSON $ genericFirstRowIO updatedBlock),
                              ("endRow", showJSON endRow),
                              ("data", showJSON blockDataTxt),
                              ("type", showJSON $ B.typeRepToColumnType $ genericTypeOfIO updatedBlock)
                            ]

                 serializedJSON = JSON.encode $ jsonData

             writeFile fileName serializedJSON)

  forM updatedColumns $ -- write out updated columns
       (\(updatedColumnSpec@(tableId, columnId), rowMappings) -> do
            let fileName = columnFileName chunkDir updatedColumnSpec -- output correct JSON data structure
                jsonData = showJSON $ toJSObject [
                            ("columnId", showJSON $ columnId),
                            ("tableId", showJSON $ tableId),
                            ("rowMappings", rowMappingsJSON)
                           ]

                rowMappingToJson ((start, end), block) = toJSObject [
                                                          ("range", showJSON $ toJSObject [("start", showJSON start), ("end", showJSON end)]),
                                                          ("blockId", showJSON block)
                                                         ]
                rowMappingsJSON = showJSON $ map rowMappingToJson rowMappings

                serializedJSON = JSON.encode $ jsonData

            writeFile fileName serializedJSON)

  unless (null updatedColumns) $ do -- if columns have been updated
       let jsonData = showJSON allBlocks
           serializedJSON = JSON.encode $ jsonData

       writeFile (tableFileName chunkDir) serializedJSON

-- instance NFData DataServerState where
--     rnf st = (rnf $ getTables st) `seq` (rnf $ getBlocks st) `seq` (rnf $ getRowMappings st) `seq` (rnf $ chunkDir st) `seq` ()

-- instance NFData GenericBlock where
--     rnf (GenericBlock rowId block) = rnf rowId `seq` rnf block `seq` ()
