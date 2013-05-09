{-# LANGUAGE TemplateHaskell, BangPatterns, CPP #-}
-- | This module defines a specialized data structure for mapping ranges of totally ordered types
-- onto values.
--
-- Many function names in this module clash, so it is best to import it @qualified@:
--
-- > import qualified Data.Tree.DisjointIntervalTree as DIT
--
-- This implementation is based on AVL trees
module Data.Tree.DisjointIntervalTree
    (-- * Data structures
     DisjointIntervalTree,

     -- * Query
     null, empty,
     lookup, lookupWithLeftBound,
     bounds, size,

     -- * Conversion
     keys, assocs, elems, fromList,

     -- * Manipulation
     insert, delete
#ifdef INCLUDE_TESTS
     , runAllTests
#endif
    )
    where

import Prelude hiding (filter, map, lookup, foldr, null)
import qualified Prelude as P
import Data.Maybe

import Control.DeepSeq

import Test.QuickCheck
import Test.QuickCheck.All

-- | A data structure for associating ranges of totally ordered types with certain values.
--
--   A @DisjointIntervalTree k v@ maps ranges of type @k@ onto values @v@. Ranges are specified as
--   2-tuples, where the range @(s, e)@, mathematically translates to the half-open range [s, e).
--
--   Thus a tree mapping (3, 9) to 10 would map the values 3, 4, 5, 6, 7, 8 (but not 9) to the value
--   10.
--
--   The mapping is allowed to have holes (i.e., ranges with no associated value) but ranges may not
--   overlap. In the case that a mapping range is inserted, it overwrites the mappings it intersects
--   (but it only overwrites these ranges within its interval). This is best explained by an
--   example. Supposed you has a @DisjointIntervalTree Int Int@ defined as
--
--   > let x = DIT.fromList [((1, 5), 10), ((9, 15), 11), ((3, 10), 12)]
--
--   Then, this is the same as writing
--
--   > let x = DIT.fromList [((1, 3), 10), ((3, 10), 12), ((10, 15), 11)]
data DisjointIntervalTree k v = Tree !k !Int (Maybe v) !(DisjointIntervalTree k v) !(DisjointIntervalTree k v) |
                                Tip
                                deriving (Show, Read, Eq)

instance (Ord k, Eq v, NFData k, NFData v) => NFData (DisjointIntervalTree k v) where
    rnf = rnf.(foldr (\k x xs -> (k,x):xs) [])

-- | An empty map, useful for initialization
empty :: DisjointIntervalTree k v
empty = Tip

-- | Returns @True@ if this map is empty
null :: DisjointIntervalTree k v -> Bool
null Tip = True
null _ = False

key :: (Ord k, Eq v) => DisjointIntervalTree k v -> k
key Tip = error "Tried to get key of tip"
key (Tree k _ _ _ _) = k

value :: (Ord k, Eq v) => DisjointIntervalTree k v -> Maybe v
value Tip = Nothing
value (Tree _ _ v _ _) = v

depth :: (Ord k, Eq v) => DisjointIntervalTree k v -> Int
depth Tip = 0
depth (Tree _ d _ _ _) = d

left, right :: (Ord k, Eq v) => DisjointIntervalTree k v -> DisjointIntervalTree k v
left (Tree _ _ _ l _) = l
right (Tree _ _ _ _ r) = r

-- | Returns all disjoint intervals associated with this map which are mapped to something. No two
-- consecutive intervals will have values that compare equal
keys :: (Ord k, Eq v) => DisjointIntervalTree k v -> [(k, k)]
keys Tip = []
keys (Tree _ _ _ Tip Tip) = []
keys (Tree ourKey _ (Just _) leftChild Tip) = (keys leftChild) ++ [(key $ maxNode leftChild, ourKey)]
keys (Tree ourKey _ (Just _) Tip rightChild) = [(ourKey, key rightChild)] ++ (keys rightChild)
keys (Tree ourKey _ (Just _) leftChild rightChild) = (keys leftChild) ++ [(key $ maxNode leftChild, ourKey), (ourKey, key $ minNode rightChild)] ++ (keys rightChild)
keys (Tree ourKey _ Nothing leftChild Tip) = (keys leftChild)
keys (Tree ourKey _ Nothing Tip rightChild) = (keys rightChild)
keys (Tree ourKey _ Nothing leftChild rightChild) = (keys leftChild) ++ (keys rightChild)

-- | Returns the number of disjoint intervals who are associated with values within this map.
size :: (Ord k, Eq v) => DisjointIntervalTree k v -> Int
size Tip = 0
size (Tree _ _ Nothing leftChild rightChild) = (size leftChild) + (size rightChild)
size (Tree _ _ (Just _) leftChild rightChild) = (size leftChild) + 1 + (size rightChild)

-- | Returns all elements associated with an interval within this map. No consecutive elements will
-- compare equal
elems :: (Ord k, Eq v) => DisjointIntervalTree k v -> [v]
elems Tip = []
elems (Tree _ _ Nothing leftChild rightChild) = (elems leftChild) ++ (elems rightChild)
elems (Tree _ _ (Just v) leftChild rightChild) = (elems leftChild) ++ [v] ++ (elems rightChild)

-- | Returns all associations in the map that have values. The return value can be passed to
-- `fromList' to create a copy of this map
assocs :: (Ord k, Eq v) => DisjointIntervalTree k v -> [((k, k), v)]
assocs Tip = []
assocs (Tree _ _ _ Tip Tip) = []
assocs (Tree ourKey _ _ leftChild Tip) =
    let leftMax = maxNode leftChild
    in (assocs leftChild) ++
       case value leftMax of -- this is an empty region
         Nothing -> []
         Just x -> [((key leftMax, ourKey), x)]
assocs (Tree ourKey _ Nothing Tip rightChild) = assocs rightChild -- we start an empty region
assocs (Tree ourKey _ (Just x) Tip rightChild) =
    let rightMin = minNode rightChild
    in [((ourKey, key rightMin), x)] ++ assocs rightChild
assocs (Tree ourKey _ v leftChild rightChild) =
    let leftMax = maxNode leftChild
        rightMin = minNode rightChild
    in (assocs leftChild) ++
           (case value leftMax of
              Nothing -> []
              Just x -> [((key leftMax, ourKey), x)]) ++
           (case v of
              Nothing -> []
              Just x -> [((ourKey, key rightMin), x)]) ++
           (assocs rightChild)

foldr :: (Ord k, Eq v) => (k -> Maybe v -> b -> b) -> b -> DisjointIntervalTree k v -> b
foldr _ z Tip = z
foldr f z (Tree k _ v l r) = let rightTree = foldr f z r
                                 ourTree = f k v rightTree
                             in foldr f ourTree l

-- | Look up a value in the tree. @lookup key tree@ returns @Just x@ if @key@ is within one of the
-- ranges in the tree and @x@ is the value associated with the range. Otherwise, the function
-- returns Nothing, indicating that no value is associated with the key.
lookup :: (Ord k, Eq v) => k -> DisjointIntervalTree k v -> Maybe v
lookup key tree = case lookupNode key tree of
                      Nothing -> Nothing
                      Just x -> value x

-- | Equivalent to `lookup', except that a tuple @(k, v)@ is returned, whose second element is the
-- value associated with the key, and whose first element is the left most bound of the range
-- that this value is associated with.
--
-- Again, an example:
--
-- >>> let x = DIT.fromList [((1, 5), 10), ((5, 8), 4), ((8, 12), 5), ((12, 20), 8)]
-- >>> lookupWithLeftBound 6 x
-- Just (5, 4)
lookupWithLeftBound :: (Ord k, Eq v) => k -> DisjointIntervalTree k v -> Maybe (k, v)
lookupWithLeftBound k tree = case lookupNode k tree of
                              Nothing -> Nothing
                              Just x -> case value x of
                                          Nothing -> Nothing
                                          Just val -> Just (key x, val)

-- | Deletes a range from the tree. All calls to `lookup' for any value in given range over the
-- returned tree will return @Nothing@.
delete :: (Ord k, Eq v) => (k, k) -> DisjointIntervalTree k v -> DisjointIntervalTree k v
delete !bounds tree = insertInBounds bounds Nothing tree

-- | @insert (start, end) value@ associates the value @v@ with all elements in the range [start,
-- end).
--
-- Calling lookup on any element in this range on the returned tree will give you back @v@.
insert :: (Ord k, Eq v) => (k, k) -> v -> DisjointIntervalTree k v -> DisjointIntervalTree k v
insert !bounds v tree = insertInBounds bounds (Just v) tree

-- | Construct a map from the given list of associations
fromList :: (Ord k, Eq v) => [((k, k), v)] -> DisjointIntervalTree k v
fromList assocs = P.foldr (uncurry insert) empty assocs

-- | Get the minimum and maximum defined boundaries. The return value @(start, end)@ is such that
-- for any x < start, @lookup x tree@ will return Nothing and that, for some element x < end for
-- which there exists no y such that x < y < end, lookup x tree is not Nothing.
bounds :: (Ord k, Eq v) => DisjointIntervalTree k v -> (k, k)
bounds tree = (key $ minNode tree, key $ maxNode tree)

-- Utility functions
lookupNode :: (Ord k, Eq v) => k -> DisjointIntervalTree k v -> Maybe (DisjointIntervalTree k v)
lookupNode !k Tip = Nothing
lookupNode !k tree@(Tree currentKey _ currentValue leftChild rightChild) =
    case () of
      _
        | k == currentKey -> Just tree
        | k < currentKey -> lookupNode k leftChild
        | k > currentKey -> case rightChild of -- Check if we're the lowest
                              Tip -> Just tree
                              _ -> let nextKey = fst $ bounds rightChild
                                   in
                                     if nextKey <= k then lookupNode k rightChild else Just tree

insertInBounds :: (Ord k, Eq v) => (k, k) -> (Maybe v) -> DisjointIntervalTree k v -> DisjointIntervalTree k v
insertInBounds (lowerBound, upperBound) v tree
    | lowerBound == upperBound = tree
    | otherwise =
        let clearedTree = cleanInterval (lowerBound, upperBound) tree
            !valueForUpperBound = lookup upperBound tree

            newUpperBoundTree = insertValue upperBound valueForUpperBound clearedTree
            newLowerBoundTree = insertValue lowerBound v rightTree

            -- Consolidate keys
            !rightTree = if (valueForUpperBound == v) then clearedTree else newUpperBoundTree -- only update the upper bound if we need to
            !leftTree =  if (valueForUpperBound == v) then deleteStartingAt upperBound newLowerBoundTree else newLowerBoundTree -- if the value is extended leftwards, get rid of the demarcation in the middle
        in leftTree

insertValue :: (Ord k, Eq v) => k -> (Maybe v) -> DisjointIntervalTree k v -> DisjointIntervalTree k v
insertValue k v (Tree currentKey currentDepth currentValue leftChild rightChild) =
    case () of
      _
          | k == currentKey -> Tree k currentDepth v leftChild rightChild
          | k < currentKey  -> let leftChild' = insertValue k v leftChild
                                   depth' = 1 + (max (depth leftChild') (depth rightChild))
                                   newTree' = Tree currentKey depth' currentValue leftChild' rightChild
                               in
                                 balance newTree'
          | k > currentKey -> let rightChild' = insertValue k v rightChild
                                  depth' = 1 + (max (depth leftChild) (depth rightChild'))
                                  newTree' = Tree currentKey depth' currentValue leftChild rightChild'
                              in
                                balance newTree'
insertValue k v Tip = Tree k 1 v Tip Tip

cleanInterval :: (Ord k, Eq v) => (k, k) -> DisjointIntervalTree k v -> DisjointIntervalTree k v
cleanInterval _ Tip = Tip
cleanInterval (lowerBound, upperBound) tree = let toDelete = P.filter (\x -> lowerBound <= x && x < upperBound) (maxValue : (P.map fst $ keys tree))
                                                  (_, maxValue) = bounds tree
                                              in
                                                P.foldr deleteStartingAt tree toDelete

deleteStartingAt :: (Ord k, Eq v) => k -> DisjointIntervalTree k v -> DisjointIntervalTree k v
deleteStartingAt _ Tip = Tip
deleteStartingAt key (Tree currentKey _ currentValue leftChild rightChild) =
    case () of
      _
        | currentKey == key -> case (leftChild, rightChild) of
                                 -- Check if either leftChild or rightChild is a tip, if so, replace this node with them and move on
                                 (Tip, Tip) -> Tip
                                 (Tip, Tree _ _ _ _ _) -> rightChild
                                 (Tree _ _ _ _ _, Tip) -> leftChild
                                 _ -> let Tree newKey _ newValue _ _ = maxNode leftChild -- Neither one was a tip :(
                                          leftChild' = deleteStartingAt newKey leftChild -- Remove the value from the left child. This shouldn't
                                                                               -- recurse since it should hit a base case
                                          depth' = 1 + (max (depth leftChild') (depth rightChild))
                                      in balance $ Tree newKey depth' newValue leftChild' rightChild
        | key < currentKey -> let leftChild' = deleteStartingAt key leftChild
                                  depth' = 1 + (max (depth leftChild') (depth rightChild))
                              in
                                balance $ Tree currentKey depth' currentValue leftChild' rightChild
        | key > currentKey -> let rightChild' = deleteStartingAt key rightChild
                                  depth' = 1 + (max (depth leftChild) (depth rightChild'))
                              in
                                balance $ Tree currentKey depth' currentValue leftChild rightChild'

-- Internal balance functions

balance :: (Ord k, Eq v) => DisjointIntervalTree k v -> DisjointIntervalTree k v
balance Tip = Tip
balance oldTree@(Tree k _ v leftChild rightChild) =
    case balanceFactor oldTree of
         -2 -> case balanceFactor rightChild of
                 -1 -> leftRotate oldTree
                 1 -> let rightChild' = rightRotate rightChild
                          tree = Tree k (1 + (max (depth leftChild) (depth rightChild'))) v leftChild rightChild'
                      in
                        leftRotate tree
                 0 -> leftRotate oldTree
                 x -> error $ "Found balance factor " ++ (show $ balanceFactor oldTree) ++ " should be " ++ (show $ realBalanceFactor oldTree)
         2 -> case balanceFactor leftChild of
                -1 -> let leftChild' = leftRotate leftChild
                          tree = Tree k (1 + (max (depth leftChild') (depth rightChild))) v leftChild' rightChild
                      in
                        rightRotate tree
                1 -> rightRotate oldTree
                0 -> rightRotate oldTree
                x -> error $ "Found balance factor " ++ (show $ balanceFactor oldTree) ++ " should be " ++ (show $ realBalanceFactor oldTree)
         _ -> oldTree
    where
      balanceFactor Tip = 0
      balanceFactor (Tree _ _ _ leftChild rightChild) = (depth leftChild) - (depth rightChild)

      realBalanceFactor tree = (realDepth $ left tree) - (realDepth $ right tree)

      realDepth Tip = 0
      realDepth (Tree _ _ _ leftChild rightChild) = 1 + (max (realDepth leftChild) (realDepth rightChild))

leftRotate, rightRotate :: (Ord k, Eq v) => DisjointIntervalTree k v -> DisjointIntervalTree k v
leftRotate (Tree k _ v leftChild rightChild) =
    let leftChild' = Tree k (1 + (max (depth leftChild) (depth $ left rightChild))) v leftChild (left rightChild)
        rightChild' = right $ rightChild
    in
      Tree (key rightChild) (1 + (max (depth leftChild') (depth rightChild'))) (value rightChild) leftChild' rightChild'

rightRotate (Tree k _ v leftChild rightChild) =
    let leftChild' = left $ leftChild
        rightChild' = Tree k (1 + (max (depth $ right leftChild) (depth rightChild))) v (right leftChild) rightChild
    in
      Tree (key leftChild) (1 + (max (depth leftChild') (depth rightChild'))) (value leftChild) leftChild' rightChild'

-- Internal minimization and maximization functions
minNode, maxNode :: (Ord k, Eq v) => DisjointIntervalTree k v -> DisjointIntervalTree k v
minNode min@(Tree k depth v Tip _) = min
minNode (Tree k depth v leftChild _) = minNode leftChild
minNode Tip = Tip

maxNode max@(Tree k depth v _ Tip) = max
maxNode (Tree k depth v _ rightChild) = maxNode rightChild
maxNode Tip = Tip

-- Unit tests

prop_depth :: DisjointIntervalTree Int String -> Bool
prop_depth tree = valid tree
    where
      depth Tip = 0
      depth (Tree _ _ _ leftChild rightChild) = 1 + (max (depth leftChild) (depth rightChild))

      valid Tip = True
      valid (Tree _ currentDepth _ leftChild rightChild) = currentDepth == (1 + (max (depth leftChild) (depth rightChild)))

prop_balance :: DisjointIntervalTree Int String -> Bool
prop_balance tree = valid $ balance tree
    where
      valid Tip = True
      valid (Tree _ _ _ leftChild rightChild) =
          let balanceFactor = (depth leftChild) - (depth rightChild)
              factorValid = balanceFactor == -1 || balanceFactor == 0 || balanceFactor == 1
          in
            factorValid && valid leftChild && valid rightChild

prop_insertRangeBegin, prop_insertRangeEnd :: DisjointIntervalTree Int String -> String -> Int -> Positive Int -> Bool
prop_insertRangeBegin tree elem a (Positive b) = (lookup a $ insert (a, a + b) elem tree) == (Just elem)
prop_insertRangeEnd tree elem a (Positive b) = (lookup (a + b) $ insert (a, a + b) elem tree) == (lookup (a + b) tree)

prop_validStructure :: DisjointIntervalTree Int String -> Bool
prop_validStructure tree = valid tree
    where
      valid Tip = True
      valid tree = let subtrees = (valid $ left tree) && (valid $ right tree)
                   in case (left tree, right tree) of
                        (Tip, Tip) -> subtrees
                        (Tip, rightChild) -> (key rightChild) > (key tree) && subtrees
                        (leftChild, Tip) -> (key leftChild) < (key tree) && subtrees
                        (leftChild, rightChild) -> (key leftChild) < (key tree) && (key rightChild) > (key tree) && subtrees

prop_testDeletion :: DisjointIntervalTree Int String -> Int -> Bool
prop_testDeletion tree elem = (lookup elem $ delete (elem, elem + 1) tree) == Nothing

prop_testAssocs :: Integer -> [Positive Integer] -> [Maybe Int] -> Bool
prop_testAssocs base offsets values =
    let lowerBounds = scanl (+) base (P.map (\(Positive x) -> x) offsets)
        bounds = zip lowerBounds (tail lowerBounds)
        ourAssocs = zip bounds values

        valuedAssocs' = P.filter (\(bounds, m) -> case m of
                                                 Just _ -> True
                                                 Nothing -> False) ourAssocs
        valuedAssocs = P.map (\(bounds, Just x) -> (bounds, x)) valuedAssocs'
        dit = fromList valuedAssocs

        calcExpAssocs (((s1, e1), x1):((s2, e2),x2):xs)
            | e1 == s2 && x1 == x2 = calcExpAssocs (((s1, e2), x1):xs)
        calcExpAssocs (x:xs) = x:(calcExpAssocs xs)
        calcExpAssocs [] = []
    in ((assocs dit) == valuedAssocs)

#ifdef INCLUDE_TESTS
runAllTests = $quickCheckAll
#endif

instance (Arbitrary k, Arbitrary v, Num k, Ord k, Eq v) => Arbitrary (DisjointIntervalTree k v) where
    arbitrary = sized tree'
        where
          tree' :: (Arbitrary k, Arbitrary v, Ord k, Num k, Eq v) => Int -> Gen (DisjointIntervalTree k v)
          tree' 0 = return Tip
          tree' n | n > 0 = do
            lowers <- vectorOf n arbitrary
            offsets <- vectorOf n arbitrary
            let offsets' = P.map abs offsets
                uppers = zipWith (+) lowers offsets'
                bounds = zip lowers uppers
            values <- sequence $ replicate n arbitrary
            return $ (P.foldr (.) id $ zipWith ($) (P.map insert bounds) values) empty