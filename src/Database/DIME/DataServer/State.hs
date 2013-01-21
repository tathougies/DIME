{-# LANGUAGE ExistentialQuantification, RankNTypes #-}
module Database.DIME.DataServer.State
    ( DataServerState(..), GenericBlock(..),
      Database.DIME.DataServer.State.empty,
      emptyBlockFromType, insertBlock, hasBlock,
      hasTable, hasColumn, hasRow,
      deleteBlock, getBlockInfo, withGenericBlock,
      modifyGenericBlock,

      getBlockCount,

      fetchColumnForRow, updateRows, establishRowMapping
    )
    where
import Database.DIME.Memory.Block as B
import Database.DIME.Memory.BlockInfo as BI
import Database.DIME

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Tree.DisjointIntervalTree as DIT
import qualified Data.List as L
import Data.Maybe
import Data.Typeable
import Data.Function

import Unsafe.Coerce

-- | data type to hold any generic block. There are convenience methods to cast (safely) to specific block types
data GenericBlock = forall a. BlockStorable a => GenericBlock !RowID !(Block a)

instance Show GenericBlock where
    show (GenericBlock r b) = "GenericBlock " ++ (show r) ++ " " ++ (show b)

-- | The data structure containing the server state
data DataServerState = DataServerState {
      -- | Maintains a mapping between table IDs and all columns (that we keep track of) that belong to that table
      getTables :: M.Map TableID (S.Set ColumnID),

      -- | Maintains a mapping between blodk identifiers and the blocks
      getBlocks :: M.Map (TableID, ColumnID, BlockID) GenericBlock,

      -- | Maps between a column identifier and a 'DisjointIntervalTree', which keeps track of which rows go in which blocks
      getRowMappings :: M.Map (TableID, ColumnID) (DIT.DisjointIntervalTree RowID BlockID)
    } deriving Show

empty :: DataServerState
empty = DataServerState M.empty M.empty M.empty

getBlockCount :: DataServerState -> Int
getBlockCount st = M.size $ getBlocks st

emptyBlockFromType :: B.ColumnType -> GenericBlock
emptyBlockFromType columnType = case columnType of
                                  B.IntColumn -> GenericBlock (RowID 0) (B.empty :: Block Int)
                                  B.StringColumn -> GenericBlock (RowID 0) (B.empty :: Block String)
                                  B.DoubleColumn -> GenericBlock (RowID 0) (B.empty :: Block Double)

-- | Insert the block but also insert the column into the table
insertBlock :: TableID -> ColumnID -> BlockID -> GenericBlock -> DataServerState -> DataServerState
insertBlock tableId columnId blockId v s = let getBlocks' = M.insert (tableId, columnId, blockId) v $ getBlocks s
                                               insertColumn Nothing = Just $ S.singleton columnId
                                               insertColumn (Just columns) = Just $ S.insert columnId columns

                                               getTables' = M.alter insertColumn tableId $ getTables s
                                           in
                                             s {getBlocks = getBlocks',
                                                getTables = getTables'}

hasBlock :: TableID -> ColumnID -> BlockID -> DataServerState -> Bool
hasBlock tableId columnId blockId DataServerState {getBlocks = blocks} = M.member (tableId, columnId, blockId) blocks

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

hasRow :: TableID -> ColumnID -> RowID -> DataServerState -> Bool
hasRow tableId columnId rowId DataServerState { getRowMappings = rowMappings} =
    let rowMappingResult = M.lookup (tableId, columnId) rowMappings
    in case rowMappingResult of
         Nothing -> error "Cannot find table and column pair"
         Just rowMapping ->
             case DIT.lookup rowId rowMapping of
               Nothing -> False
               _ -> True

deleteBlock :: TableID -> ColumnID -> BlockID -> DataServerState -> DataServerState
deleteBlock tableId columnId blockId s =
    let blockInfo = getBlockInfo tableId columnId blockId s
    in s {getBlocks = M.delete (tableId, columnId, blockId) $ getBlocks s,
          getRowMappings = M.adjust (DIT.delete (firstRow blockInfo, lastRow blockInfo)) (tableId, columnId) $ getRowMappings s}

-- | Updates the rows in the given column with the given values. The rows must be sorted, and
-- | The columns must have the correct type or bad things will happen!
updateRows :: TableID -> ColumnID -> [RowID] -> [ColumnValue] -> DataServerState -> DataServerState
updateRows tableId columnId rowIds values state =
    let Just rowMappings = M.lookup (tableId, columnId) $ getRowMappings state
        idsAndValues = zip rowIds values
        blockIdForRow rowId = fromJust $ DIT.lookup rowId rowMappings
        blockUpdateDescrs = L.groupBy ((==) `on` (blockIdForRow.fst)) idsAndValues -- Group rows from the same block together

        updateBlock rowIdsAndValues st = -- Update the block corresponding to these indices and return the newly altered state
            let (rowId, firstValue) = head rowIdsAndValues
                blockId = fromJust $ DIT.lookup rowId rowMappings
                blockStartIndex = genericFirstRow $ fromJust $ M.lookup (tableId, columnId, blockId) $ getBlocks st

                rowIdsAndValues' = map (\(x, ColumnValue y) -> (fromIntegral $ x - blockStartIndex, unsafeCoerce y)) rowIdsAndValues

                typeCheck block = (typeOf $ block #! 0) == (withColumnValue typeOf firstValue)

                applyUpdate block = if typeCheck block then B.update block rowIdsAndValues' else
                                        error "Incorrect type supplied for block"
            in
              st { getBlocks = M.adjust (modifyGenericBlock id applyUpdate) (tableId, columnId, blockId) $ getBlocks st }
    in
      foldr updateBlock state (blockUpdateDescrs :: [[ (RowID, ColumnValue)]])

fetchColumnForRow :: TableID -> ColumnID -> RowID -> DataServerState -> ColumnValue
fetchColumnForRow tableId columnId rowId state =
    let Just rowMapping = M.lookup (tableId, columnId) $ getRowMappings state
        Just blockId = DIT.lookup rowId rowMapping
        Just block = M.lookup (tableId, columnId, blockId) $ getBlocks state
        offsetIndex = rowId - (genericFirstRow block)
    in
      withGenericBlock (\block' -> ColumnValue $ block' #! (fromIntegral offsetIndex)) block

getBlockInfo :: TableID -> ColumnID -> BlockID -> DataServerState -> BlockInfo
getBlockInfo tableId columnId blockId DataServerState {getBlocks = blocks} =
    let block = fromJust $ M.lookup k blocks
        firstRow = genericFirstRow block

        k = (tableId, columnId, blockId)
    in
      BI.empty { firstRow = firstRow, lastRow = firstRow + (fromIntegral $ genericLength block),
                 blockType = B.typeRepToColumnType $ genericTypeOf block
               }

establishRowMapping :: (TableID, ColumnID) -> (RowID, RowID) -> BlockID -> DataServerState -> DataServerState
establishRowMapping blockKey bounds blockId st@(DataServerState {getRowMappings = rowMappings}) =
    let updateRowMapping :: Maybe (DIT.DisjointIntervalTree RowID BlockID) -> Maybe (DIT.DisjointIntervalTree RowID BlockID)
        updateRowMapping value = let tree = maybe DIT.empty id value
                                 in Just $ DIT.insert bounds blockId tree
        rowMapping' = M.alter updateRowMapping blockKey rowMappings
    in
      st { getRowMappings = rowMapping' }

genericLength :: GenericBlock -> Int
genericLength = withGenericBlock B.length

genericTypeOf :: GenericBlock -> TypeRep
genericTypeOf = withGenericBlock (\x -> typeOf $ x #! 0)

genericFirstRow :: GenericBlock -> RowID
genericFirstRow (GenericBlock r _) = r

withGenericBlock :: (forall a. BlockStorable a => Block a -> b) ->  GenericBlock -> b
withGenericBlock f (GenericBlock _ b) = f b

-- | Apply transformation functions on the fields of a generic block
-- | Captures type variables, so it makes the type system happy
modifyGenericBlock :: (RowID -> RowID) -> (forall a. BlockStorable a => Block a -> Block a) -> GenericBlock -> GenericBlock
modifyGenericBlock rowIndexF dataF (GenericBlock r b) = GenericBlock (rowIndexF r) (dataF b)