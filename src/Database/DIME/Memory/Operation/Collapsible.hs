{-# LANGUAGE RecordWildCards #-}
module Database.DIME.Memory.Operation.Collapsible
    (BlockCollapsible(..),
     CollapseOperation(..),
     verifyCollapseOpTypesWithBlock) where

    import Control.Monad

    import Data.Binary
    import Data.Typeable
    import Data.Int

    import Database.DIME.Memory.Block as B

    data CollapseOperation = Sum |
                             Product |
                             forall a. BlockStorable a => Convolve (Block a)

    mkConvolve :: [ColumnValue] -> CollapseOperation
    mkConvolve columnValues =
        let valuesType = withColumnValue typeOf $ head columnValues
        in case typeRepToColumnType valuesType of
             IntColumn -> Convolve (B.fromList columnValues :: Block Int)
             DoubleColumn -> Convolve (B.fromList columnValues :: Block Double)
             StringColumn -> Convolve (B.fromList columnValues :: Block String)

    verifyCollapseOpTypesWithBlock :: BlockStorable a => CollapseOperation -> Block a -> Bool
    verifyCollapseOpTypesWithBlock (Convolve otherBlock) ourBlock =
        let convolveBlockType = typeOf (otherBlock #! 0)
            ourBlockType = typeOf (ourBlock #! 0)
        in convolveBlockType == ourBlockType

    sumTag, productTag, convolveTag :: Int8
    sumTag = 1
    productTag = 2
    convolveTag = 3

    instance Binary CollapseOperation where
        put Sum = put sumTag
        put Product = put productTag
        put (Convolve values) = do
          put convolveTag
          putColumnValues $ map ColumnValue (B.toList values)

        get = do
          tag <- (get :: Get Int8)
          case tag of
            1 {- sumTag -} -> return Sum
            2 {- productTag -} -> return Product
            3 {- convolveTag -} -> do
                     columnValues <- getColumnValues
                     return $ mkConvolve columnValues

    instance Show CollapseOperation where
        show Sum = "Sum"
        show Product = "Product"
        show (Convolve blk) = "Convolve (" ++ show blk ++ ")"

    instance Read CollapseOperation where
        readsPrec prec x = (readParen (prec > app_prec)
                           (\r -> do
                              (t, rest) <- lex r
                              case t of
                                "Sum" -> return (Sum, rest)
                                "Product" -> return (Product, rest)
                                "Convolve" -> do
                                       (values, rest') <- readsPrec (app_prec + 1) rest
                                       return (mkConvolve values, rest'))) x
         where app_prec = 10

    class BlockStorable a => BlockCollapsible a where
        collapseOp2Func :: CollapseOperation -> Block a -> a

        collapseBlock :: CollapseOperation -> Int -> Block a -> Maybe (Block a)
        collapseBlock collapseOp collapseLength block =
            let lastResultIndex = B.length block - collapseLength
                resultLength = lastResultIndex + 1

                resultBase = B.resize resultLength $ B.empty

                collapseFunc = collapseOp2Func collapseOp

                resultList = map (\i -> (i, collapseFunc $ B.slice i (i + collapseLength - 1) block)) [0..lastResultIndex]
            in Just $ resultBase `B.update` resultList