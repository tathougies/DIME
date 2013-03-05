{-# LANGUAGE FlexibleInstances #-}
module Database.DIME.Memory.Operation.String where

    import Database.DIME.Memory.Block
    import Database.DIME.Memory.Operation.Mappable

    import qualified Data.Vector as V

    instance BlockMappable String where
        -- Note that both of these return lazy streams. Eventually, the computation will be forced
        -- when the issuer issues the force command
        mapBlock _ _ = Nothing

        isMappable _ = True