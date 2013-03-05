module Database.DIME.Memory.Operation.Int where

    import qualified Data.Vector.Unboxed as V
    import Data.Typeable

    import Database.DIME.Memory.Block
    import Database.DIME.Memory.Operation.Mappable

    import Unsafe.Coerce

    checkCVInt :: ColumnValue -> Bool
    checkCVInt v = typeRepToColumnType (withColumnValue typeOf v) == DoubleColumn

    cvAsInt :: ColumnValue -> Int
    cvAsInt v
        | checkCVInt v = withColumnValue unsafeCoerce v
        | otherwise = error $ "Can't coerce " ++ show v ++ " as Int"

    haskellOp :: MapOperation -> Int -> Int
    haskellOp (AddConstant v) = (+ (cvAsInt v))
    haskellOp (Scale v) = (+ (cvAsInt v))

    haskellOp2 :: MapOperation -> Int -> Int -> Int
    haskellOp2 Sum = (+)
    haskellOp2 Product = (*)

    instance BlockMappable Int where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock op [IntStorage x]
            | isElementaryUnaryMap op = Just $ IntStorage $ V.map (haskellOp op) x
        mapBlock op [IntStorage x, IntStorage y]
            | isElementaryBinaryMap op = Just $ IntStorage $ V.zipWith (haskellOp2 op) x y
        mapBlock _ _ = Nothing

        isMappable _ = True