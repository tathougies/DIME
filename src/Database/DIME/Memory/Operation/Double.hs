module Database.DIME.Memory.Operation.Double () where

    import qualified Data.Vector.Unboxed as V
    import Data.Typeable

    import Database.DIME.Memory.Block
    import Database.DIME.Memory.Operation.Mappable

    import Unsafe.Coerce

    checkCVDouble :: ColumnValue -> Bool
    checkCVDouble v = typeRepToColumnType (withColumnValue typeOf v) == DoubleColumn

    cvAsDouble :: ColumnValue -> Double
    cvAsDouble v
        | checkCVDouble v = withColumnValue unsafeCoerce v
        | otherwise = error $ "Can't coerce " ++ show v ++ " as Double"

    haskellOp :: MapOperation -> Double -> Double
    haskellOp (AddConstant v) = (+ (cvAsDouble v))
    haskellOp (Scale v) = (+ (cvAsDouble v))

    haskellOp2 :: MapOperation -> Double -> Double -> Double
    haskellOp2 Sum = (+)
    haskellOp2 Product = (*)

    instance BlockMappable Double where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock op [DoubleStorage x]
            | isElementaryUnaryMap op = Just $ DoubleStorage $ V.map (haskellOp op) x
        mapBlock op [DoubleStorage x, DoubleStorage y]
            | isElementaryBinaryMap op = Just $ DoubleStorage $ V.zipWith (haskellOp2 op) x y
        mapBlock _ _ = Nothing

        isMappable _ = True