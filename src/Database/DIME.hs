{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.DIME
    (
     BlockID(..), TableID(..), ColumnID(..),
     RowID(..)
    ) where

import Data.Int

import Data.Binary
import Control.Monad

newtype BlockID = BlockID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral)
newtype TableID = TableID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral)
newtype ColumnID = ColumnID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral)
newtype RowID = RowID Int64
    deriving (Ord, Eq, Show, Read, Real, Enum, Num, Integral)

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
