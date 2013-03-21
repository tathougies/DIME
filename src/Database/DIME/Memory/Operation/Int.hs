module Database.DIME.Memory.Operation.Int where

    import qualified Data.Vector.Unboxed as V
    import Data.Typeable

    import qualified Database.DIME.Memory.Operation.Mappable as MapOp
    import qualified Database.DIME.Memory.Operation.Collapsible as CollapseOp
    import Database.DIME.Memory.Operation.Mappable hiding (Sum, Product, AddConstant, Scale)
    import Database.DIME.Memory.Operation.Collapsible hiding (Sum, Product, Convolve)
    import Database.DIME.Memory.Block

    import Unsafe.Coerce

    checkCVInt :: ColumnValue -> Bool
    checkCVInt v = typeRepToColumnType (withColumnValue typeOf v) == DoubleColumn

    cvAsInt :: ColumnValue -> Int
    cvAsInt v
        | checkCVInt v = withColumnValue unsafeCoerce v
        | otherwise = error $ "Can't coerce " ++ show v ++ " as Int"

    haskellOp :: MapOperation -> Int -> Int
    haskellOp (MapOp.AddConstant v) = (+ (cvAsInt v))
    haskellOp (MapOp.Scale v) = (+ (cvAsInt v))

    haskellOp2 :: MapOperation -> Int -> Int -> Int
    haskellOp2 MapOp.Sum = (+)
    haskellOp2 MapOp.Product = (*)

    instance BlockMappable Int where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock op [IntStorage x]
            | isElementaryUnaryMap op = Just $ IntStorage $ V.map (haskellOp op) x
        mapBlock op [IntStorage x, IntStorage y]
            | isElementaryBinaryMap op = Just $ IntStorage $ V.zipWith (haskellOp2 op) x y
        mapBlock _ _ = Nothing

        isMappable _ = True

    instance BlockCollapsible Int where
        collapseOp2Func CollapseOp.Sum (IntStorage v) = V.sum v
        collapseOp2Func CollapseOp.Product (IntStorage v) = V.product v
        collapseOp2Func (CollapseOp.Convolve storage) (IntStorage v) =
            case unsafeCoerce storage of
              IntStorage values -> V.sum $ V.zipWith (*) values v