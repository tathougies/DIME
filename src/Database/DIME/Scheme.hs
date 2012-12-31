module Scheme (
) where

data Table = Table { getColumnNames :: [String],
                     getColumnRefs :: [ColumnID],
                     getColumnTypes :: [ColumnType]}

insertRow :: Table -> 
