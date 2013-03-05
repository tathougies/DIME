{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.DIME
    (
     BlockID(..), TableID(..), ColumnID(..),
     RowID(..), BlockSpec(..)
    ) where

import Data.Int

import Data.Binary
import Control.Monad
import Control.DeepSeq

import Text.JSON

newtype BlockID = BlockID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData)
newtype TableID = TableID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData)
newtype ColumnID = ColumnID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData)
newtype RowID = RowID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral, JSON, NFData)

data BlockSpec = BlockSpec TableID ColumnID BlockID
    deriving (Ord, Eq, Show, Read)

instance Binary BlockID where
    put (BlockID blockId) = put blockId
    get = liftM BlockID get

instance Binary TableID where
    put (TableID tableId) = put tableId
    get = liftM TableID get

instance Binary ColumnID where
    put (ColumnID columnId) = put columnId
    get = liftM ColumnID get

instance Binary RowID where
    put (RowID rowId) = put rowId
    get = liftM RowID get

instance Binary BlockSpec where
    put (BlockSpec tId cId bId) = do
      put tId
      put cId
      put bId
    get = do
      tId <- get
      cId <- get
      bId <- get
      return $ BlockSpec tId cId bId

instance JSON BlockSpec where
    showJSON (BlockSpec tableId columnId blockId) = showJSON $ (tableId, columnId, blockId)
    readJSON jsValue = case (readJSON jsValue :: Result (TableID, ColumnID, BlockID)) of
                         Ok (tableId, columnId, blockId) -> Ok $ BlockSpec tableId columnId blockId
                         Error e -> Error e

instance NFData BlockSpec where
    rnf (BlockSpec tId cId bId) = rnf tId `seq` rnf cId `seq` rnf bId `seq` ()