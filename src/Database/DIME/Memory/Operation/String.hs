{-# LANGUAGE FlexibleInstances #-}
module Database.DIME.Memory.Operation.String where

    import qualified Database.DIME.Memory.Operation.Mappable as MapOp
    import qualified Database.DIME.Memory.Operation.Collapsible as CollapseOp
    import Database.DIME.Memory.Operation.Mappable hiding (Sum, Product, AddConstant, Scale)
    import Database.DIME.Memory.Operation.Collapsible hiding (Sum, Product, Convolve)
    import Database.DIME.Memory.Block

    import qualified Data.Array as A

    instance BlockMappable String where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock _ _ = Nothing

        isMappable _ = True

    instance BlockCollapsible String where
        collapseOp2Func CollapseOp.Sum (StringStorage v) = foldl (++) "" $ A.elems v
        collapseOp2Func _ _ = error "String does not support product or convolve"