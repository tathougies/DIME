{-# LANGUAGE BangPatterns, TypeFamilies, FlexibleInstances, ExistentialQuantification, RankNTypes, FlexibleContexts #-}
module Database.DIME.Memory.Block
    (
     ColumnValue(..),
     ColumnType(..),
     BlockStorable(..),
     Block (..),
     typeRepToColumnType,
     withColumnValue
    ) where

import Prelude hiding (length)

import Data.List hiding (length)
import Data.List.Split
import Data.Default
import Data.Binary
import Data.Typeable

import Data.Array.IArray ( (!), (//) )

import qualified Data.Vector.Unboxed as V
import qualified Data.Array.IArray as A

import Control.Monad
import Control.DeepSeq

import Test.QuickCheck.All

import Text.JSON

blockLength = 65536 -- blocks store 65k entries

-- Block classes

{-| A type that can store different types of data
    The s type is the storage type
    The d type is the data type stored in the storage
-}
class (Show s, Read s, Binary s, Typeable s, NFData (Block s)) => BlockStorable s where
    data Block s

    -- | Get data at an index
    infixl 8 #!
    (#!) :: Block s -> Int -> s

    -- | Update the values at the indices given to the data values given
    update :: Block s -> [(Int, s)] -> Block s

    -- | Zero-length block
    empty :: Block s

    -- | Length of block
    length :: Block s -> Int

    -- | Take slice of block
    slice :: Int -> Int -> Block s -> Block s

    -- | Add an element to the end of a block
    append :: s -> Block s -> Block s

    -- | default value
    defaultBlockElement :: s

    -- | convert block to list of values
    toList :: Block s -> [s]
    toList blk = map (blk #!) [0..length blk - 1]

    -- | resize block. Default implementation uses slice, append, and defaultBlockElement
    resize :: Int -> Block s -> Block s
    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = let ret = foldr append block $ replicate (newSize - (Database.DIME.Memory.Block.length block)) defaultBlockElement
                                                                in
                                                                  ret `seq` ret
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

instance BlockStorable [Char] where
    newtype Block [Char] = StringStorage (A.Array Int [Char])

    {-# INLINE (#!) #-}
    (StringStorage storage) #! index = storage ! index

    update (StringStorage storage) assocs = strictness `seq`
                            StringStorage $ storage // assocs
        where strictness = foldl' (seq.snd) (0, "") assocs

    empty = StringStorage $ A.listArray (0, -1) []
    length (StringStorage storage) = let (upper, lower) = A.bounds storage
                                     in lower - upper + 1

    append e s@(StringStorage v) = StringStorage $ A.listArray (0, Database.DIME.Memory.Block.length s) $ (A.elems v) ++ [e]
    slice b e (StringStorage v) = StringStorage $ A.listArray (0, e - b - 1) $ map (v A.!) [b..e-1]
    defaultBlockElement = ""

    toList (StringStorage a) = A.elems a

    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = case block of
                                                                  StringStorage a -> StringStorage $ A.listArray (0, newSize - 1) $ (A.elems a) ++ repeat defaultBlockElement
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

instance NFData (Block [Char]) where
    rnf (StringStorage a) = rnf a

instance BlockStorable Int where
    newtype Block Int = IntStorage (V.Vector Int)

    {-# INLINE (#!) #-}
    (IntStorage storage) #! index = storage V.! index
    update (IntStorage storage) = IntStorage.((V.//) storage)
    empty = IntStorage $ V.empty
    length (IntStorage storage) = V.length storage

    append e (IntStorage v) = IntStorage $ V.snoc v e
    slice b e (IntStorage v) = IntStorage $ V.slice b e v
    defaultBlockElement = 0

    toList (IntStorage v) = V.toList v

    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = case block of
                                                                  IntStorage v -> IntStorage $ V.fromList $ (V.toList v) ++ replicate (newSize - V.length v) defaultBlockElement
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

instance NFData (Block Int) where
    rnf (IntStorage a) = rnf a

instance BlockStorable Double where
    newtype Block Double = DoubleStorage (V.Vector Double)

    {-# INLINE (#!) #-}
    (DoubleStorage storage) #! index = storage V.! index
    update (DoubleStorage storage) = DoubleStorage.((V.//) storage)
    empty = DoubleStorage $ V.empty
    length (DoubleStorage storage) = V.length storage

    append e (DoubleStorage v) = DoubleStorage $ V.snoc v e
    slice b e (DoubleStorage v) = DoubleStorage $ V.slice b e v
    defaultBlockElement = 0.0

    toList (DoubleStorage v) = V.toList v

    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = case block of
                                                                  DoubleStorage v -> DoubleStorage $ V.fromList $ (V.toList v) ++ replicate (newSize - V.length v) defaultBlockElement
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

instance NFData (Block Double) where
    rnf (DoubleStorage a) = rnf a

instance (BlockStorable a, Show a) => Show (Block a) where
    show x = "Block [" ++ (intercalate "," $ map (show.(x #!)) [0..(Database.DIME.Memory.Block.length x) - 1]) ++ "]"

-- | Generic type for storing column values
data ColumnValue = forall a. BlockStorable a => ColumnValue a

instance Show ColumnValue where
    show (ColumnValue a) = "ColumnValue " ++ t ++ " " ++ (show a)
                           where t = case (show $ typeOf a) of
                                       "[Char]" -> "String"
                                       x -> x

instance Read ColumnValue where
    readsPrec prec x =  (readParen (prec > app_prec)
                        (\r -> do
                           ("ColumnValue", rest) <- lex r
                           (t, rest') <- lex rest
                           let liftCV :: BlockStorable a => [(a, String)] -> [(ColumnValue, String)]
                               liftCV = map (\(a, s) -> (ColumnValue a, s))
                           (value, rest'') <- case t of
                                          "Int" -> liftCV (readsPrec (app_prec + 1) rest' :: [(Int, String)])
                                          "Double" -> liftCV (readsPrec (app_prec + 1) rest' :: [(Double, String)])
                                          "String" -> liftCV (readsPrec (app_prec + 1) rest' :: [(String, String)])
                           return (value, rest''))) x
        where app_prec = 10
              up_prec = 5

instance Binary ColumnValue where
    put (ColumnValue a) = do
      put (show $ typeOf a)
      put a
    get = do
      typeName <- (get :: Get String)
      case typeName of
        "Int" -> liftM ColumnValue (get :: Get Int)
        "Double" -> liftM ColumnValue (get :: Get Double)
        "[Char]" -> liftM ColumnValue (get :: Get String)

-- | Data class for column types
data ColumnType = IntColumn | StringColumn | DoubleColumn
                deriving (Show, Read, Eq, Enum, Ord)

instance JSON ColumnType where
    showJSON IntColumn = showJSON $ toJSString "int"
    showJSON StringColumn = showJSON $ toJSString "string"
    showJSON DoubleColumn = showJSON $ toJSString "double"
    showJSON x = error $ "Can't serialize " ++ show x ++ " column type to JSON"

    readJSON (JSString x)
        | fromJSString x == "int" = Ok IntColumn
        | fromJSString x == "string" = Ok StringColumn
        | fromJSString x == "double" = Ok DoubleColumn
    readJSON value = Error $ "Invalid ColumnType: " ++ Text.JSON.encode value

instance Binary ColumnType where
    put t = put $ fromEnum t
    get = do
      t <- get :: Get Int
      return $ toEnum t

typeRepToColumnType :: TypeRep -> ColumnType
typeRepToColumnType t
    | t == typeOf (0::Int)     = IntColumn
    | t == typeOf (0::Double)  = DoubleColumn
    | t == typeOf (""::String) = StringColumn
    | otherwise                = error $ "Unknown column type: " ++ (show t)

withColumnValue :: (forall a. BlockStorable a => a -> b) -> ColumnValue -> b
withColumnValue f (ColumnValue v) = f v