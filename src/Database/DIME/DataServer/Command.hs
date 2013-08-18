{-# LANGUAGE GeneralizedNewtypeDeriving, RecordWildCards #-}
module Database.DIME.DataServer.Command
    ( Command(..),
      QueryKey(..)
    ) where
import Database.DIME
import Database.DIME.Memory.Block (BlockStorable, ColumnValue, ColumnType,
                                   getColumnValues, putColumnValues)
import Database.DIME.Memory.Operation.Mappable
import Database.DIME.Memory.Operation.Collapsible

import qualified Data.Text as T
import Data.Binary
import Data.Int
import Data.Typeable

import Control.Monad

import Language.Flow.Execution.Types -- for Binary Text intance

import Network.Socket (SockAddr)

import Text.JSON

updateRowsTag, fetchRowsTag, newBlockTag, deleteBlockTag, blockInfoTag, mapTag, collapseTag, runQueryTag, forceComputationTag :: Int8
runQueryTag = 0
updateRowsTag = 1
fetchRowsTag = 2
newBlockTag = 3
deleteBlockTag = 4
blockInfoTag = 5
mapTag = 6
collapseTag = 7
forceComputationTag = 8

-- | Identifies running queries. Used by nodes to make requests in order to see a consistent state
newtype QueryKey = QueryKey Int64
    deriving (Show, Read, Ord, Eq, Num, Enum, JSON, Binary)

-- | Represents a data node command
data Command = UpdateRows TableID [BlockRowID] [ColumnID] [[ColumnValue]] |
               -- ^ Updates the columns given in block-level rows specified with the given
               -- values. The values array is in row major format; that is, each element of the
               -- ColumnValues array represents a row of values.
               --
               -- Possible responses:
               --
               --    [`Ok'] The command succeeded and the rows are now updated
               --
               --    [`InconsistentTypes'] The command failed because the types of the ColumnValues
               --    did not match the types of the columns given.

               FetchRows TableID [(BlockRowID,  BlockRowID)] [ColumnID] |
               -- ^ Get rows from a block. The TableID gives the id of the table, the ColumnIDs are
               -- the columns from which to fetch and the BlockRowID list gives the list of block
               -- ranges to fetch. The returned rows correspond to the BlockRowIDs in the
               -- concatenation of all the ranges.
               --
               -- Possible responses:
               --
               --     [`FetchRowsResponse' @values@] @values@ is a list of lists. The inner most
               --     lists contain entries corresponding to each column's values in the row. In the
               --     outer list, the values are returned in the order specified by the the
               --     BlockRowID list.
               --
               --     [`InconsistentArguments'] The table given does not exist, the columns
               --     supplied do not exist, or the ranges specified do not exist.

               -- Block commands
               NewBlock BlockSpec BlockRowID BlockRowID ColumnType |
               -- ^ @NewBlock spec start end blockType@ Creates a new block with the given
               -- identifier @spec@, whose values range from @start@ to @end@ and who are of type
               -- @blockType@
               --
               -- Possible responses:
               --
               --     [`Ok'] The block was created with the given specifications and now exists.
               --
               --     [`BlockAlreadyExists'] The block already exists. Nothing was changed on the server.

               DeleteBlock BlockSpec |
               -- ^ Delete the block specified. The command does not fail in the sense that, after
               -- the command is run, the block specified in the command will not exist (either
               -- because it never existed or because it was removed)
               --
               -- Possible responses:
               --
               --    [`Ok'] The block specified was deleted.
               --
               --    [`BlockAreadyExists'] The block specified did not exist.

               BlockInfo BlockSpec |
               -- ^ Fetch meta-data about the specified block.
               --
               -- Possible responses:
               --
               --    [`BlockInfoResponse' @blockInfo@] The requested info. The @blockInfo@ returned
               --    has type `BlockInfo'.
               --
               --    [`BlockDoesNotExist'] The block requested does not exist.

               TransferBlock BlockSpec BlockSpec SockAddr |

               Map MapOperation [BlockSpec] BlockSpec |
               Collapse {
                   collapseOperation :: CollapseOperation,
                   collapseBlockSpec :: BlockSpec,
                   collapseResultBlock :: BlockSpec,
                   collapseBlockSuffix :: [ColumnValue],
                   collapseLength :: Int} |
               ForceComputation BlockSpec |
               {-
               Reduce ReduceOperation TableID ColumnID BlockID |
               { -}

               RunQuery QueryKey T.Text
             deriving Show

instance Binary Command where
    put (UpdateRows tableId rowIds columnIds columnValues) = do
                          put updateRowsTag
                          put tableId
                          put rowIds
                          put columnIds
                          put columnValues
    put (FetchRows tableId rowIds columnIds) = do
                          put fetchRowsTag
                          put tableId
                          put rowIds
                          put columnIds
    put (NewBlock blockSpec startIndex endIndex blockType) = do
                          put newBlockTag
                          put blockSpec
                          put startIndex
                          put endIndex
                          put blockType
    put (DeleteBlock blockSpec) = do
                          put deleteBlockTag
                          put blockSpec
    put (BlockInfo blockSpec) = do
                          put blockInfoTag
                          put blockSpec
    put (Map op inputs output) = do
                          put mapTag
                          put op
                          put inputs
                          put output
    put Collapse {..} = do
                          put collapseTag
                          put collapseOperation
                          put collapseBlockSpec
                          put collapseResultBlock
                          putColumnValues collapseBlockSuffix
                          put collapseLength
    put (ForceComputation blockSpec) = do
                          put forceComputationTag
                          put blockSpec
    put (RunQuery queryKey progTxt) = do
                          put runQueryTag
                          put queryKey
                          put progTxt

    get = do
      tag <- (get :: Get Int8)
      case tag of
        0 {- runQueryTag -} -> getRunQuery
        1 {- updateRowsTag -} -> getUpdateRows
        2 {- fetchRowsTag -} -> getFetchRows
        3 {- newBlockTag -} -> getNewBlock
        4 {- deleteBlockTag -} -> getDeleteBlock
        5 {- blockInfoTag -} -> getBlockInfo
        6 {- mapTag -} -> getMap
        7 {- collapseTag -} -> getCollapse
        8 {- forceComputationTag -} -> getForceComputation
     where
       getRunQuery = do
            queryKey <- get
            progTxt <- get
            return $ RunQuery queryKey progTxt
       getUpdateRows = do
            tableId <- get
            rowIds <- get
            columnIds <- get
            columnValues <- get
            return $ UpdateRows tableId rowIds columnIds columnValues
       getFetchRows = do
            tableId <- get
            rowIds <- get
            columnIds <- get
            return $ FetchRows tableId rowIds columnIds
       getNewBlock = do
            blockSpec <- get
            rowStartId <- get
            rowEndId <- get
            blockType <- get
            return $ NewBlock blockSpec rowStartId rowEndId blockType
       getDeleteBlock = liftM DeleteBlock get
       getBlockInfo = liftM BlockInfo get
       getMap = do
            op <- get
            inputs <- get
            output <- get
            return $ Map op inputs output
       getCollapse = do
            collapseOperation <- get
            collapseBlockSpec <- get
            collapseResultBlock <- get
            collapseBlockSuffix <- getColumnValues
            collapseLength <- get
            return Collapse {..}
       getForceComputation = do
            blockSpec <- get
            return $ ForceComputation blockSpec