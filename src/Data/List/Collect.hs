module Data.List.Collect where

import Data.List
import Data.Function

collect :: (Ord a, Eq a) => [(a, b)] -> [(a, [b])]
collect pairs = let grouped = groupBy ((==) `on` fst) pairs
                in map (\((key, v):kvs) -> (key, v:(map snd kvs))) grouped