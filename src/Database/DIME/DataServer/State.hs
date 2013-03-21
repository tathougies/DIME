{-# LANGUAGE ExistentialQuantification, RankNTypes, BangPatterns, RecordWildCards #-}
module Database.DIME.DataServer.State
    ( DataServerState(..), GenericBlock(..),
      mkEmptyServerState,
      emptyBlockFromType, insertBlock, hasBlock,
      mapServerBlock, collapseServerBlock, forceCompute,
      hasTable, hasColumn, hasRow,
      deleteBlock, getBlockInfo, withGenericBlock,
      modifyGenericBlock,

      getBlockCount,

      fetchColumnForRow, updateRows, establishRowMapping,

      dumpState
    )
    where
import qualified Codec.Compression.GZip as GZip

import qualified Control.Exception as E
import Control.DeepSeq
import Control.Concurrent.STM
import Control.Monad

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
import Data.IORef
import Data.Maybe
import Data.Typeable
import Data.Function
import Data.Char

import System.IO.Unsafe
import System.Directory
import System.FilePath

import Text.JSON as JSON

import Unsafe.Coerce

-- | data type to hold any generic block. There are convenience methods to cast (safely) to specific block types
data GenericBlock = forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => GenericBlock !BlockRowID !(B.Block a)

instance Show GenericBlock where
    show (GenericBlock r b) = "GenericBlock " ++ (show r) ++ " " ++ (show b)

-- | The data structure containing the server state
data DataServerState = DataServerState {
      -- | Maintains a mapping between table IDs and all columns (that we keep track of) that belong to that table
      getTables :: M.Map TableID (S.Set ColumnID),

      -- | Maintains a mapping between blodk identifiers and the blocks
      getBlocks :: M.Map BlockSpec GenericBlock,

      -- | Maps between a column identifier and a 'DisjointIntervalTree', which keeps track of which rows go in which blocks
      getRowMappings :: M.Map (TableID, ColumnID) (DIT.DisjointIntervalTree BlockRowID BlockID),

      -- | A list of blocks modified since the last dump
      getModifiedBlocks :: S.Set BlockSpec,

      -- | The directory in which to store chunks and mapping file
      getChunkDir :: FilePath
    } deriving Show

blockFileName :: FilePath -> BlockSpec -> FilePath
blockFileName chunkDir (BlockSpec tableId columnId blockId) = chunkDir </> "blk_" ++ showAsI tableId ++ "_" ++ showAsI columnId ++ "_" ++ showAsI blockId

columnFileName :: FilePath -> (TableID, ColumnID) -> FilePath
columnFileName chunkDir (tableId, columnId) = chunkDir </> "col_" ++ showAsI tableId ++ "_" ++ showAsI columnId

tableFileName :: FilePath -> FilePath
tableFileName chunkDir = chunkDir </> "tables"

showAsI :: Integral a => a -> String
showAsI x = show (fromIntegral x :: Int)

mkEmptyServerState :: FilePath -> IO (TVar DataServerState)
mkEmptyServerState chunkDir = do
  let emptyState = DataServerState M.empty M.empty M.empty S.empty chunkDir
  -- check if files exist
  stateExists <- doesFileExist $ tableFileName chunkDir
  finalState <- if stateExists then  restoreStateFromChunks emptyState
                else return emptyState
  newTVarIO finalState

restoreStateFromChunks :: DataServerState -> IO DataServerState
restoreStateFromChunks state = do
  let chunkDir = getChunkDir state

  stateRef <- newIORef state -- for simplicity

  -- parse tables json
  tablesJSONData <- readFile $ tableFileName chunkDir
  let allBlocks = case (JSON.decode tablesJSONData :: Result [BlockSpec]) of
                    Error e -> fail $ "Could not read tables data: " ++ e
                    Ok blocks -> blocks

      allColumns = S.toList $ S.fromList $ map (\(BlockSpec tableId columnId _) -> (tableId, columnId)) allBlocks -- get all unique columns

  forM allColumns $
       (\columnSpec -> do
          columnData <- readFile $ columnFileName chunkDir columnSpec
          let (tableId, columnId, rowMappings) = case JSON.decode columnData of
                  Ok (JSObject columnDict) -> parseColumn columnDict
                  Ok x -> error $ "Couldn't read column " ++ show columnSpec ++ ": bad type"
                  Error e -> error $ "Couldn't read column " ++ show columnSpec ++ ": " ++ e
          modifyIORef stateRef $
              (\st -> let columnsForTable = maybe S.empty id $ M.lookup tableId $ getTables st
                          columnsForTable' = S.insert columnId columnsForTable
                          getTables' = M.insert tableId columnsForTable' $ getTables st -- add the column to the

                          getRowMappings' = M.insert (tableId, columnId) rowMappings $ getRowMappings st
                      in
                        st { getTables = getTables',
                             getRowMappings = getRowMappings' }))

  forM allBlocks $
       (\blockSpec -> do
          blockData <- readFile $ blockFileName chunkDir blockSpec

          let blk = case JSON.decode blockData of
                      Ok (JSObject blockDict) -> parseBlock blockDict
                      Ok x -> error $ "Couldn't read block " ++ show blockSpec ++ ": bad type"
                      Error e -> error $ "Couldn't read block " ++ show blockSpec ++ ": " ++ e

          modifyIORef stateRef $
              (\st -> let getBlocks' = M.insert blockSpec blk $ getBlocks st
                      in st { getBlocks = getBlocks' }))
  ret <- readIORef stateRef
  E.evaluate $ rnf ret -- evaluate fully
  return ret
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

emptyBlockFromType :: B.ColumnType -> GenericBlock
emptyBlockFromType columnType = case columnType of
                                  B.IntColumn -> GenericBlock (BlockRowID 0) (B.empty :: B.Block Int)
                                  B.StringColumn -> GenericBlock (BlockRowID 0) (B.empty :: B.Block String)
                                  B.DoubleColumn -> GenericBlock (BlockRowID 0) (B.empty :: B.Block Double)

-- | Insert the block but also insert the column into the table
insertBlock :: BlockSpec -> GenericBlock -> DataServerState -> DataServerState
insertBlock blockSpec@(BlockSpec tableId columnId _) v s =
    let getBlocks' = M.insert blockSpec v $ getBlocks s
        insertColumn Nothing = Just $ S.singleton columnId
        insertColumn (Just columns) = Just $ S.insert columnId columns

        getTables' = M.alter insertColumn tableId $ getTables s

        getModifiedBlocks' = S.insert blockSpec $ getModifiedBlocks s
    in
      s {getBlocks = getBlocks',
         getTables = getTables',
         getModifiedBlocks = getModifiedBlocks'}

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

hasRow :: TableID -> ColumnID -> BlockRowID -> DataServerState -> Bool
hasRow tableId columnId rowId DataServerState { getRowMappings = rowMappings} =
    let rowMappingResult = M.lookup (tableId, columnId) rowMappings
    in case rowMappingResult of
         Nothing -> error "Cannot find table and column pair"
         Just rowMapping ->
             case DIT.lookup rowId rowMapping of
               Nothing -> False
               _ -> True

deleteBlock :: BlockSpec -> DataServerState -> DataServerState
deleteBlock blockSpec@(BlockSpec tableId columnId _) s =
    let blockInfo = getBlockInfo blockSpec s
    in s {getBlocks = M.delete blockSpec $ getBlocks s,
          getRowMappings = M.adjust (DIT.delete (firstRow blockInfo, lastRow blockInfo)) (tableId, columnId) $ getRowMappings s,
          getModifiedBlocks = S.insert blockSpec $ getModifiedBlocks s}

-- | Updates the rows in the given column with the given values. The rows must be sorted, and
-- | The columns must have the correct type or bad things will happen!
updateRows :: TableID -> ColumnID -> [BlockRowID] -> [B.ColumnValue] -> DataServerState -> DataServerState
updateRows tableId columnId rowIds values state =
    let Just rowMappings = M.lookup (tableId, columnId) $ getRowMappings state
        idsAndValues = zip rowIds values
        blockIdForRow rowId = fromJust $ DIT.lookup rowId rowMappings
        blockUpdateDescrs = L.groupBy ((==) `on` (blockIdForRow.fst)) idsAndValues -- Group rows from the same block together

        updateBlock rowIdsAndValues st = -- Update the block corresponding to these indices and return the newly altered state
            let (rowId, firstValue) = head rowIdsAndValues
                !blockId = fromJust $ DIT.lookup rowId rowMappings
                blockSpec = BlockSpec tableId columnId blockId
                blockStartIndex = genericFirstRow $ fromJust $ M.lookup blockSpec $ getBlocks st

                rowIdsAndValues' = map (\(x, B.ColumnValue y) -> (fromIntegral $ x - blockStartIndex, unsafeCoerce y)) rowIdsAndValues

                typeCheck block = (typeOf $ block #! 0) == (B.withColumnValue typeOf firstValue)

                applyUpdate block = if typeCheck block then B.update block rowIdsAndValues' else
                                        error "Incorrect type supplied for block"
            in
              st { getBlocks = M.adjust (modifyGenericBlock id applyUpdate) (BlockSpec tableId columnId blockId) $ getBlocks st,
                   getModifiedBlocks = S.insert blockSpec $ getModifiedBlocks st }
    in
      foldr updateBlock state (blockUpdateDescrs :: [[ (BlockRowID, B.ColumnValue)]])

fetchColumnForRow :: TableID -> ColumnID -> BlockRowID -> DataServerState -> B.ColumnValue
fetchColumnForRow tableId columnId rowId state =
    let Just rowMapping = M.lookup (tableId, columnId) $ getRowMappings state
        Just blockId = DIT.lookup rowId rowMapping
        Just block = M.lookup (BlockSpec tableId columnId blockId) $ getBlocks state
        offsetIndex = rowId - (genericFirstRow block)
    in withGenericBlock (\block' -> B.ColumnValue $ block' #! (fromIntegral offsetIndex)) block

getBlockInfo :: BlockSpec -> DataServerState -> BlockInfo
getBlockInfo blockSpec DataServerState {getBlocks = blocks} =
    let block = fromJust $ M.lookup blockSpec blocks
        firstRow = genericFirstRow block
    in
      BI.empty { firstRow = firstRow, lastRow = firstRow + (fromIntegral $ genericLength block) - BlockRowID 1,
                 blockType = B.typeRepToColumnType $ genericTypeOf block
               }

forceCompute :: BlockSpec -> DataServerState -> DataServerState
forceCompute blockSpec st =

    st { getBlocks = M.alter (Just . (modifyGenericBlock id B.forceCompute) . fromJust) blockSpec $ getBlocks st }

mapServerBlock :: MapOperation -> [BlockSpec] -> BlockSpec -> DataServerState -> Maybe DataServerState
mapServerBlock op [] _ state = Nothing
mapServerBlock op inputs output@(BlockSpec tableId columnId blockId) state =
    let firstBlockType = genericTypeOf $ head inputBlocks
        firstRow = genericFirstRow $ head inputBlocks
        blocksTypeCheck = all ((== firstBlockType) . genericTypeOf) inputBlocks

        inputBlocks = map (fromJust . (flip M.lookup $ getBlocks state)) inputs
    in if blocksTypeCheck then
           let newBlock = newBlockFromType firstBlockType

               newBlockFromType t
                   | t == typeOf (undefined :: Int) =
                       case mapBlock op $ map (fromJust . genericCoerceInt) inputBlocks of
                         Nothing -> Nothing
                         Just x -> Just $ GenericBlock firstRow x
                   | t == typeOf (undefined :: Double) =
                       case  mapBlock op $ map (fromJust . genericCoerceDouble) inputBlocks of
                         Nothing -> Nothing
                         Just x -> Just $ GenericBlock firstRow x
                   | t == typeOf (undefined :: String) =
                       case mapBlock op $ map (fromJust . genericCoerceString) inputBlocks of
                         Nothing -> Nothing
                         Just x -> Just $ GenericBlock firstRow x
            in case newBlock of
                 Nothing -> Nothing
                 Just newBlock' ->
                     let state' = insertBlock output newBlock' state
                         state'' = establishRowMapping (tableId, columnId) (firstRow, firstRow + BlockRowID (fromIntegral $ genericLength newBlock')) blockId state'
                     in Just state''
       else Nothing

collapseServerBlock :: Command -> DataServerState -> Maybe DataServerState
collapseServerBlock Collapse {..} state =
    let Just genericInputBlock = M.lookup collapseBlockSpec $ getBlocks state
    in usingGenericBlock genericInputBlock $
       \inputBlock ->
         let finalInputBlock = inputBlock `B.append` (B.fromList collapseBlockSuffix)

             blockType = genericTypeOf genericInputBlock
             firstRow = genericFirstRow genericInputBlock
         in case collapseBlock collapseOperation collapseLength finalInputBlock of
           Nothing -> Nothing
           Just x ->
             let resultBlock = GenericBlock firstRow x
                 state' = insertBlock collapseResultBlock resultBlock state
                 BlockSpec resultTableId resultColumnId resultBlockId = collapseResultBlock
                 state'' = establishRowMapping (resultTableId, resultColumnId) (firstRow, firstRow + BlockRowID(fromIntegral $ genericLength resultBlock)) resultBlockId state'
             in Just state''

establishRowMapping :: (TableID, ColumnID) -> (BlockRowID, BlockRowID) -> BlockID -> DataServerState -> DataServerState
establishRowMapping blockKey bounds blockId st@(DataServerState {getRowMappings = rowMappings}) =
    let updateRowMapping :: Maybe (DIT.DisjointIntervalTree BlockRowID BlockID) -> Maybe (DIT.DisjointIntervalTree BlockRowID BlockID)
        updateRowMapping value = let tree = maybe DIT.empty id value
                                 in Just $ DIT.insert bounds blockId tree
        rowMapping' = M.alter updateRowMapping blockKey rowMappings
    in st { getRowMappings = rowMapping' }

genericLength :: GenericBlock -> Int
genericLength = withGenericBlock B.length

genericTypeOf :: GenericBlock -> TypeRep
genericTypeOf = withGenericBlock (\x -> typeOf $ x #! 0)

genericFirstRow :: GenericBlock -> BlockRowID
genericFirstRow (GenericBlock r _) = r

withGenericBlock :: (forall a. (BM.BlockMappable a, BC.BlockCollapsible a) => B.Block a -> b) ->  GenericBlock -> b
withGenericBlock f (GenericBlock _ b) = f b

usingGenericBlock = flip withGenericBlock

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

dumpState :: TVar DataServerState -> IO ()
dumpState stateVar = do
  (updatedBlocks, updatedColumns, allBlocks, chunkDir) <-
      atomically $ do -- read a ilst of recently changed chunks plus their related data
          state <- readTVar stateVar
          let modifiedBlocks = S.toList $ getModifiedBlocks state
              allModifiedColumns = L.sort $ map (\(BlockSpec tableId columnId _) -> (tableId, columnId)) modifiedBlocks
              modifiedColumns = L.nub allModifiedColumns
              updatedBlocks = map (\blockSpec ->
                                      case M.lookup blockSpec (getBlocks state) of
                                        Nothing -> error $ "Deleted " ++ show blockSpec ++ " not handled"
                                        Just block -> (blockSpec, block)) modifiedBlocks
              updatedColumns = map (\columnSpec ->
                                        case M.lookup columnSpec (getRowMappings state) of
                                          Nothing -> error $ "Deleted " ++ show columnSpec ++ " not handled"
                                          Just rowMapping -> (columnSpec, DIT.assocs rowMapping)) modifiedColumns

          writeTVar stateVar $ state {getModifiedBlocks = S.empty} -- empty out modified blocks

          -- the row ranges of modified columns should be written out again

          -- all modified blocks must be written out
          return (updatedBlocks, updatedColumns, M.keys $ getBlocks state, getChunkDir state)

  -- TODO Before writing all these out, we should write a transaction log to the disk

  forM updatedBlocks $ -- write out updated block data
       (\(updatedBlockSpec@(BlockSpec tableId columnId blockId), updatedBlock) -> do
            let fileName = blockFileName chunkDir updatedBlockSpec
                blockData = withGenericBlock (Bin.encode . B.toList) updatedBlock
                blockDataTxt = B64.encode $ BS.concat $ LBS.toChunks $ GZip.compress $ blockData

                endRow =  (fromIntegral $ genericFirstRow updatedBlock) + genericLength updatedBlock - 1
                jsonData = showJSON $ toJSObject [
                            ("tableId", showJSON tableId),
                            ("columnId", showJSON columnId),
                            ("blockId", showJSON blockId),
                            ("startRow", showJSON $ genericFirstRow updatedBlock),
                            ("endRow", showJSON endRow),
                            ("data", showJSON blockDataTxt),
                            ("type", showJSON $ B.typeRepToColumnType $ genericTypeOf updatedBlock)
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

instance NFData DataServerState where
    rnf st = (rnf $ getTables st) `seq` (rnf $ getBlocks st) `seq` (rnf $ getRowMappings st) `seq` (rnf $ getChunkDir st) `seq` ()

instance NFData GenericBlock where
    rnf (GenericBlock rowId block) = rnf rowId `seq` rnf block `seq` ()
