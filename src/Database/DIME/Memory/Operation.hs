module Database.DIME.Memory.Operation
    (calcBlockAlignments) where

import Database.DIME

import Database.DIME.Memory.Operation.Mappable
import Database.DIME.Memory.Operation.Collapsible

import Database.DIME.Memory.Operation.Double
import Database.DIME.Memory.Operation.Int
import Database.DIME.Memory.Operation.String

inBetween x (start, end) = x >= start && x < end

{-# SPECIALIZE calcBlockAlignments :: [((RowID, RowID), a)] -> [((RowID, RowID), a)] -> [((RowID, RowID), a, a)] #-}
calcBlockAlignments :: Ord x => [((x, x), a)] -> [((x, x), a)] -> [((x, x), a, a)]
calcBlockAlignments _ [] = []
calcBlockAlignments [] _ = []
calcBlockAlignments ((x, ox):xs) ((y, oy):ys)
        | x `completelyBefore` y = calcBlockAlignments xs ((y, oy):ys)
        | x `completelyAfter` y = calcBlockAlignments ((x, ox):xs) ys
        | x `startsInButEndsOutside` y = ((boundStart x, boundEnd y), ox, oy):(calcBlockAlignments (((boundEnd y, boundEnd x), ox):xs) ys)
        | x `startsOutsideButEndsIn` y = ((boundStart y, boundEnd x), ox, oy):(calcBlockAlignments xs (((boundEnd x, boundEnd y), oy):ys))
        | x `completelyContains` y = (y, ox, oy):(calcBlockAlignments (((boundEnd y, boundEnd x), ox):xs) ys)
        | x `completelyContainedBy` y = (x, ox, oy):(calcBlockAlignments xs (((boundEnd x, boundEnd y), oy):ys))

    where
      boundStart = fst
      boundEnd = snd

      completelyBefore x y = boundEnd x <= boundStart y
      completelyAfter = flip completelyBefore
      startsInButEndsOutside x y = boundStart x `inBetween` y && boundEnd x >= boundEnd y
      startsOutsideButEndsIn x y = boundStart x < boundStart y && boundEnd x `inBetween` y
      completelyContains x y = boundStart y `inBetween` x && boundEnd y `inBetween` x
      completelyContainedBy = flip completelyContains

      inBetween x (yStart, yEnd) = x >= yStart && x < yEnd
