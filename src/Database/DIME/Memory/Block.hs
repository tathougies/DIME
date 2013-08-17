{-# LANGUAGE BangPatterns, TypeFamilies, FlexibleInstances, ExistentialQuantification, RankNTypes, FlexibleContexts, DeriveDataTypeable, StandaloneDeriving #-}
module Database.DIME.Memory.Block
    (
     ColumnValue(..),
     ColumnType(..),
     BlockStorable(..),
     Block (..),
     fromList, fromListUnboxed,
     typeRepToColumnType,
     withColumnValue,
     getColumnValues,
     putColumnValues
    ) where

import qualified Prelude as Prelude
import Prelude hiding (length)

import Control.Applicative hiding (empty)

import Data.List hiding (length)
import Data.List.Split
import Data.Default
import Data.Binary
import Data.Typeable
import Data.Ratio

import Data.Array.IArray ( (!), (//) )

import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as MV
import qualified Data.Array.IArray as A
import qualified Data.Array.IO as AIO

import Control.Monad
import Control.DeepSeq

import Test.QuickCheck.All

import Text.JSON

import Unsafe.Coerce

blockLength = 65536 -- blocks store 65k entries

-- Block classes

{-| A type that can store different types of data
    The s type is the storage type
    The d type is the data type stored in the storage
-}
class (Show s, Read s, Binary s, Typeable s, JSON s, NFData (Block s), Typeable1 BlockIO) => BlockStorable s where
    data Block s
    data BlockIO s

    -- | Monadic indexing
    newM    :: Int -> IO (BlockIO s)
    indexM  :: BlockIO s -> Int -> IO s
    updateM :: BlockIO s -> Int -> s -> IO ()
    lengthM :: BlockIO s -> IO Int
    forceComputeM :: BlockIO s -> IO ()
    toListM :: BlockIO s -> IO [s]
    freeze :: BlockIO s -> IO (Block s)
    thaw :: Block s -> IO (BlockIO s)

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
    snoc :: s -> Block s -> Block s

    -- | Append two blocks together
    append :: Block s -> Block s -> Block s
    append prefix suffix = foldr snoc prefix (toList suffix)

    -- | default value
    defaultBlockElement :: s

    -- | convert block to list of values
    toList :: Block s -> [s]
    toList blk = map (blk #!) [0..length blk - 1]

    -- | resize block. Default implementation uses slice, snoc, and defaultBlockElement
    resize :: Int -> Block s -> Block s
    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = let ret = foldr snoc block $ replicate (newSize - (Database.DIME.Memory.Block.length block)) defaultBlockElement
                                                                in
                                                                  ret `seq` ret
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

    forceCompute :: Block s -> Block s
    forceCompute x = x -- for future

deriving instance Typeable1 BlockIO

instance BlockStorable [Char] where
    newtype Block [Char] = StringStorage (A.Array Int [Char])
    newtype BlockIO [Char] = StringStorageIO (AIO.IOArray Int [Char])

    newM size = StringStorageIO <$> AIO.newArray (0, size - 1) defaultBlockElement

    indexM (StringStorageIO a) i = AIO.readArray a i
    updateM (StringStorageIO a) i e = AIO.writeArray a i e
    lengthM (StringStorageIO a) = AIO.getBounds a >>= (\(0, maxIndex) -> return $! maxIndex + 1)

    {-# INLINE (#!) #-}
    (StringStorage storage) #! index = storage ! index

    update (StringStorage storage) assocs = strictness `seq`
                            StringStorage $ storage // assocs
        where strictness = foldl' (seq.snd) (0, "") assocs

    empty = StringStorage $ A.listArray (0, -1) []
    length (StringStorage storage) = let (upper, lower) = A.bounds storage
                                     in lower - upper + 1

    snoc e s@(StringStorage v) = StringStorage $ A.listArray (0, Database.DIME.Memory.Block.length s) $ (A.elems v) ++ [e]
    append p@(StringStorage prefix) s@(StringStorage suffix) =
        StringStorage $ A.listArray (0, Database.DIME.Memory.Block.length s + Database.DIME.Memory.Block.length p) $ (A.elems prefix) ++ (A.elems suffix)
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
    newtype BlockIO Int = IntStorageIO (MV.IOVector Int)

    newM size = IntStorageIO <$> MV.replicate size defaultBlockElement
    indexM (IntStorageIO a) i = MV.unsafeRead a i
    updateM (IntStorageIO a) i e = MV.unsafeWrite a i e
    lengthM (IntStorageIO a) = return (MV.length a)

    {-# INLINE (#!) #-}
    (IntStorage storage) #! index = storage V.! index
    update (IntStorage storage) = IntStorage.((V.//) storage)
    empty = IntStorage $ V.empty
    length (IntStorage storage) = V.length storage

    snoc e (IntStorage v) = IntStorage $ V.snoc v e
    append (IntStorage prefix) (IntStorage suffix) = IntStorage $ prefix V.++ suffix
    slice b e (IntStorage v) = IntStorage $ V.slice b (e - b + 1) v
    defaultBlockElement = 0

    toList (IntStorage v) = V.toList v

    resize newSize block
        | (Database.DIME.Memory.Block.length block) < newSize = case block of
                                                                  IntStorage v -> IntStorage $ V.fromList $ (V.toList v) ++ replicate (newSize - V.length v) defaultBlockElement
        | (Database.DIME.Memory.Block.length block) > newSize = slice 0 newSize block -- Slice
        | otherwise = block

fromListUnboxed :: BlockStorable a => [a] -> Block a
fromListUnboxed l =
    let blockBase = resize (Prelude.length l) empty
    in blockBase `update` (zip [0..] l)

fromList :: BlockStorable a => [ColumnValue] -> Block a
fromList values =
    let defaultResultElement =
            let constraint :: BlockStorable a => Block a -> a -- type system trickery
                constraint _ = defaultBlockElement
            in constraint resultBlock

        blockType = typeOf defaultResultElement
        typesCheck = all ((== blockType) . (withColumnValue typeOf)) values
        resultBlock = fromListUnboxed $ map (withColumnValue unsafeCoerce) values
    in if typesCheck then resultBlock else error "types don't check in fromList"

instance NFData (Block Int) where
    rnf (IntStorage a) = rnf a

instance BlockStorable Double where
    newtype Block Double = DoubleStorage (V.Vector Double)
    newtype BlockIO Double = DoubleStorageIO (MV.IOVector Double)

    newM size = DoubleStorageIO <$> MV.replicate size defaultBlockElement
    indexM (DoubleStorageIO a) i = MV.unsafeRead a i
    updateM (DoubleStorageIO a) i e = MV.unsafeWrite a i e
    lengthM (DoubleStorageIO a) = return (MV.length a)

    {-# INLINE (#!) #-}
    (DoubleStorage storage) #! index = storage V.! index
    update (DoubleStorage storage) = DoubleStorage.((V.//) storage)
    empty = DoubleStorage $ V.empty
    length (DoubleStorage storage) = V.length storage

    snoc e (DoubleStorage v) = DoubleStorage $ V.snoc v e
    append (DoubleStorage prefix) (DoubleStorage suffix) = DoubleStorage $ prefix V.++ suffix
    slice b e (DoubleStorage v) = DoubleStorage $ V.slice b (e - b + 1) v
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

instance JSON ColumnValue where
    showJSON (ColumnValue v) = showJSON v
    readJSON (JSRational _ d) =
        if denominator d == 1 then
            Ok $ ColumnValue (fromIntegral $ numerator d :: Int)
        else
            Ok $ ColumnValue (fromRational d :: Double)
    readJSON (JSString s) = Ok $ ColumnValue (fromJSString s)
    readJSON _ = Error "Bad type for ColumnValue"

getColumnValues :: Get [ColumnValue]
getColumnValues = do
  length <- (get :: Get Int)
  if length /= 0
     then do
       typeName <- (get :: Get String)
       case typeName of
         "Int" -> replicateM length (liftM ColumnValue (get :: Get Int))
         "Double" -> replicateM length (liftM ColumnValue (get :: Get Double))
         "[Char]" -> replicateM length (liftM ColumnValue (get :: Get String))
     else return []

putColumnValues :: [ColumnValue] -> Put
putColumnValues [] = put (0 :: Int)
putColumnValues xs = do
  put (Prelude.length xs :: Int)
  put (withColumnValue (show . typeOf) (head xs))
  forM_ xs $ withColumnValue put

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