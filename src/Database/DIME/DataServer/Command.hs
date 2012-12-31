module Database.DIME.DataServer.Command
    ( Command(..)
    ) where
import Database.DIME
import Database.DIME.Memory.Block (ColumnValue, ColumnType)

import Data.Binary
import Data.Int
import Data.Typeable

import Control.Monad

updateRowsTag, fetchRowsTag, newBlockTag, deleteBlockTag, blockInfoTag :: Int8
updateRowsTag = 1
fetchRowsTag = 2
newBlockTag = 3
deleteBlockTag = 4
blockInfoTag = 5

-- | Represents a data node command
data Command = UpdateRows TableID [RowID] [ColumnID] [[ColumnValue]] |
               FetchRows TableID [RowID] [ColumnID] |

               -- Block commands
               NewBlock TableID ColumnID BlockID RowID RowID ColumnType |
               DeleteBlock TableID ColumnID BlockID |
               BlockInfo TableID ColumnID BlockID
             deriving (Show, Read)

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
    put (NewBlock tableId columnId blockId startIndex endIndex blockType) = do
                          put newBlockTag
                          put tableId
                          put columnId
                          put blockId
                          put startIndex
                          put endIndex
                          put blockType
    put (DeleteBlock tableId columnId blockId) = do
                          put deleteBlockTag
                          put tableId
                          put columnId
                          put blockId
    put (BlockInfo tableId columnId blockId) = do
                          put blockInfoTag
                          put tableId
                          put columnId
                          put blockId

    get = do
      tag <- (get :: Get Int8)
      case tag of
        1 {- updateRowsTag -} -> getUpdateRows
        2 {- fetchRowsTag -} -> getFetchRows
        3 {- newBlockTag -} -> getNewBlock
        4 {- deleteBlockTag -} -> getDeleteBlock
        5 {- blockInfoTag -} -> getBlockInfo
     where
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
            tableId <- get
            columnId <- get
            blockId <- get
            rowStartId <- get
            rowEndId <- get
            blockType <- get
            return $ NewBlock tableId columnId blockId rowStartId rowEndId blockType
       getDeleteBlock = do
            tableId <- get
            columnId <- get
            blockId <- get
            return $ DeleteBlock tableId columnId blockId
       getBlockInfo = do
            tableId <- get
            columnId <- get
            blockId <- get
            return $ BlockInfo tableId columnId blockId