{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.DIME.DataServer.Command
    ( Command(..),
      QueryKey(..)
    ) where
import Database.DIME
import Database.DIME.Memory.Block (ColumnValue, ColumnType)
import Database.DIME.Memory.Operation.Mappable

import qualified Data.Text as T
import Data.Binary
import Data.Int
import Data.Typeable

import Control.Monad

import Language.Flow.Execution.Types -- for Binary Text intance

import Text.JSON

updateRowsTag, fetchRowsTag, newBlockTag, deleteBlockTag, blockInfoTag, mapTag, runQueryTag, forceComputationTag :: Int8
runQueryTag = 0
updateRowsTag = 1
fetchRowsTag = 2
newBlockTag = 3
deleteBlockTag = 4
blockInfoTag = 5
mapTag = 6
forceComputationTag = 7

-- | Identifies running queries. Used by nodes to make requests in order to see a consistent state
newtype QueryKey = QueryKey Int64
    deriving (Show, Read, Ord, Eq, Num, Enum, JSON, Binary)

-- | Represents a data node command
data Command = UpdateRows TableID [RowID] [ColumnID] [[ColumnValue]] |
               FetchRows TableID [(RowID,  RowID)] [ColumnID] |

               -- Block commands
               NewBlock BlockSpec RowID RowID ColumnType |
               DeleteBlock BlockSpec |
               BlockInfo BlockSpec |

               TransferBlock BlockSpec BlockSpec String |

               Map MapOperation [BlockSpec] BlockSpec |
               ForceComputation BlockSpec |
               --Calc CalcOperation BlockSpec |
               {-
               Reduce ReduceOperation TableID ColumnID BlockID |
               { -}

               RunQuery QueryKey T.Text
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
        7 {- forceComputationTag -} -> getForceComputation
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
       getForceComputation = do
            blockSpec <- get
            return $ ForceComputation blockSpec