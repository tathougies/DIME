module RangeTree
    (Range(...), empty) where

import Test.QuickCheck.All

{-| A generic range type -}
data Ord k => Range k = Range { lowerBound, upperBound :: k}
                      deriving (Eq)

inRange :: Ord k => Range k -> k -> Bool
inRange range value = (lowerBound range) <= value && value < (upperBound range)

{-| A type that can map disjoint ranges of orderable values to values.
-}
data Ord k => RangeTree k v = Branch {
      getKey :: !(Range k),
      getBalance :: {-# UNPACK #-} !Int,
      getValue :: !v,
      getLeft, getRight :: !(RangeTree k v)
    } | Tip

empty :: Ord k => RangeTree k v
empty = Tip

{-| Associates the second parameter with the first range. Ranges in the range tree are rewritten to
  fully associate the given range to the value.
-}
insert :: Ord k => Range k -> v -> RangeTree k v -> RangeTree k v
insert range value tree = -- Find where the key goes

runTests = $quickCheckAll