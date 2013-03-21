module Database.DIME.Memory.Operation.Double () where

    import Control.Monad
    import Control.Monad.Trans

    import qualified Data.Vector.Unboxed as V
    import Data.Typeable

    import qualified Database.DIME.Memory.Operation.Mappable as MapOp
    import qualified Database.DIME.Memory.Operation.Collapsible as CollapseOp
    import Database.DIME.Memory.Operation.Mappable hiding (Sum, Product, AddConstant, Scale)
    import Database.DIME.Memory.Operation.Collapsible hiding (Sum, Product, Convolve)
    import Database.DIME.Memory.Block

    import Language.Flow.Execution.Types
    import Language.Flow.Execution.GMachine

    import System.IO.Unsafe

    import Unsafe.Coerce

    checkCVDouble :: ColumnValue -> Bool
    checkCVDouble v = typeRepToColumnType (withColumnValue typeOf v) == DoubleColumn

    cvAsDouble :: ColumnValue -> Double
    cvAsDouble v
        | checkCVDouble v = withColumnValue unsafeCoerce v
        | otherwise = error $ "Can't coerce " ++ show v ++ " as Double"

    haskellOp :: MapOperation -> Double -> Double
    haskellOp (MapOp.AddConstant v) = (+ (cvAsDouble v))
    haskellOp (MapOp.Scale v) = (+ (cvAsDouble v))

    haskellOp2 :: MapOperation -> Double -> Double -> Double
    haskellOp2 MapOp.Sum = (+)
    haskellOp2 MapOp.Product = (*)

    instance BlockMappable Double where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock op [DoubleStorage x]
            | isElementaryUnaryMap op = Just $ DoubleStorage $ V.map (haskellOp op) x
        mapBlock op [DoubleStorage x, DoubleStorage y]
            | isElementaryBinaryMap op = Just $ DoubleStorage $  V.zipWith (haskellOp2 op) x y
        mapBlock (GMachineMap state fnAddr) xs =
            let DoubleStorage v1 = head xs
                vs = map (\(DoubleStorage x) -> x) xs

                gState = unsafePerformIO $ thawState () state
                gCode = gmachineFrozenCode state

                runGMachineOp gSt i = unsafePerformIO $ do
                                       res <- runGMachine (gmachineFn i) $ gSt { gmachineCode = gCode }
                                       case res of
                                        Left e -> fail $ show e
                                        Right (st, res) -> return res

                gmachineFn i = do
                  let vals = map (V.! i) vs
                  forM_ vals $ \val -> do
                                    addr <- allocGraphCell
                                    writeGraph addr $ mkGeneric $ DoubleConstant val
                                    pushStack addr
                  retVal <- continue
                  when (not $ isDouble retVal) $ throwError "Did not return double"
                  return $ asDouble retVal
            in Just $ DoubleStorage $ V.generate (V.length v1) (runGMachineOp gState)
        mapBlock _ _ = Nothing

        isMappable _ = True

    instance BlockCollapsible Double where
        collapseOp2Func CollapseOp.Sum (DoubleStorage v) = V.sum v
        collapseOp2Func CollapseOp.Product (DoubleStorage v) = V.product v
        collapseOp2Func (CollapseOp.Convolve storage) (DoubleStorage v) =
            case unsafeCoerce storage of
              DoubleStorage values -> V.sum $ V.zipWith (*) values v