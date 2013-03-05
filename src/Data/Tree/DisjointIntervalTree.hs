{-# LANGUAGE TemplateHaskell, BangPatterns #-}
module Data.Tree.DisjointIntervalTree
    (DisjointIntervalTree,
     empty, lookup, keys, assocs, elems,
     insert, delete,
     fromList,
     runAllTests)
    where

import Prelude hiding (filter, map, lookup, foldr)
import qualified Prelude as P
import Data.Maybe

import Control.DeepSeq

import Test.QuickCheck
import Test.QuickCheck.All

data DisjointIntervalTree k v = Tree !k !Int (Maybe v) !(DisjointIntervalTree k v) !(DisjointIntervalTree k v) |
                                Tip
                                deriving (Show, Read, Eq)

instance (Ord k, Eq v, NFData k, NFData v) => NFData (DisjointIntervalTree k v) where
    rnf = rnf.(foldr (\k x xs -> (k,x):xs) [])

empty :: DisjointIntervalTree k v
empty = Tip

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

keys :: (Ord k, Eq v) => DisjointIntervalTree k v -> [(k, k)]
keys Tip = []
keys (Tree _ _ _ Tip Tip) = []
keys (Tree ourKey _ _ leftChild Tip) = (keys leftChild) ++ [(key leftChild, ourKey)]
keys (Tree ourKey _ _ Tip rightChild) = [(ourKey, key rightChild)] ++ (keys rightChild)
keys (Tree ourKey _ _ leftChild rightChild) = (keys leftChild) ++ [(key leftChild, ourKey), (ourKey, key rightChild)] ++ (keys rightChild)

elems :: (Ord k, Eq v) => DisjointIntervalTree k v -> [v]
elems Tip = []
elems (Tree _ _ Nothing leftChild rightChild) = (elems leftChild) ++ (elems rightChild)
elems (Tree _ _ (Just v) leftChild rightChild) = (elems leftChild) ++ [v] ++ (elems rightChild)

assocs :: (Ord k, Eq v) => DisjointIntervalTree k v -> [((k, k), v)]
assocs Tip = []
assocs (Tree _ _ _ Tip Tip) = []
assocs (Tree ourKey _ _ leftChild Tip) = (assocs leftChild) ++
                                         case value leftChild of
                                           Nothing -> []
                                           Just x -> [((key leftChild, ourKey), x)]
assocs (Tree ourKey _ _ Tip rightChild) = (case value rightChild of
                                             Nothing -> []
                                             Just x -> [((ourKey, key rightChild), x)]) ++ assocs rightChild
assocs (Tree ourKey _ v leftChild rightChild) = (assocs leftChild) ++
                                                (case value leftChild of
                                                   Nothing -> []
                                                   Just x -> [((key leftChild, ourKey), x)]) ++
                                                (case v of
                                                   Nothing -> []
                                                   Just x -> [((ourKey, key rightChild), x)]) ++
                                                (assocs rightChild)

foldr :: (Ord k, Eq v) => (k -> Maybe v -> b -> b) -> b -> DisjointIntervalTree k v -> b
foldr _ z Tip = z
foldr f z (Tree k _ v l r) = let rightTree = foldr f z r
                                 ourTree = f k v rightTree
                             in foldr f ourTree l

lookup :: (Ord k, Eq v) => k -> DisjointIntervalTree k v -> Maybe v
lookup key tree = case lookupNode key tree of
                      Nothing -> Nothing
                      Just x -> value x

delete :: (Ord k, Eq v) => (k, k) -> DisjointIntervalTree k v -> DisjointIntervalTree k v
delete !bounds tree = insertInBounds bounds Nothing tree

insert :: (Ord k, Eq v) => (k, k) -> v -> DisjointIntervalTree k v -> DisjointIntervalTree k v
insert !bounds v tree = insertInBounds bounds (Just v) tree

fromList :: (Ord k, Eq v) => [((k, k), v)] -> DisjointIntervalTree k v
fromList assocs = P.foldr (uncurry insert) empty assocs

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
            !leftTree = if (lookup lowerBound clearedTree == v) then rightTree else newLowerBoundTree
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

runAllTests = $quickCheckAll

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