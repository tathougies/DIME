module Database.DIME.Memory.Operation.Mappable where

    import Control.Monad

    import Data.Typeable
    import Data.Binary
    import Data.Int

    import Database.DIME.Memory.Block

    import Language.Flow.Execution.Types

    data MapOperation = Sum |
                        AddConstant ColumnValue |
                        Product |
                        Scale ColumnValue |
                        GMachineMap GMachineFrozenState GMachineAddress
         deriving (Show, Read)

    sumTag, addConstantTag, productTag, scaleTag, gmachineTag :: Int8
    sumTag = 1
    addConstantTag = 2
    productTag = 3
    scaleTag = 4
    gmachineTag = 5

    instance Binary MapOperation where
        put Sum = put sumTag
        put (AddConstant v) = do
          put addConstantTag
          put v
        put Product = put productTag
        put (Scale v) = do
          put scaleTag
          put v
        put (GMachineMap state fnAddr) = do
          put gmachineTag
          put state
          put fnAddr

        get = do
          tag <- (get :: Get Int8)
          case tag of
            1 {- sumTag -} -> return Sum
            2 {- addConstantTag -} -> liftM AddConstant get
            3 {- productTag -} -> return Product
            4 {- scaleTag -} -> liftM Scale get
            5 {- gmachineTag -} -> liftM2 GMachineMap get get

    isElementaryBinaryMap, isElementaryUnaryMap :: MapOperation -> Bool

    -- | Returns true if the MapOperation given can be used as a binary operation, and is not a GCodeOp
    isElementaryBinaryMap Sum = True
    isElementaryBinaryMap Product = True
    isElementaryBinaryMap _ = False

    -- | Returns true if the MapOperation given can be used as a unary operation, and is not a GCodeOp
    isElementaryUnaryMap (AddConstant _) = True
    isElementaryUnaryMap (Scale _) = True
    isElementaryUnaryMap _ = False

    -- | Class to support map operations over blocks. All block types should implement this
    --   even if they don't support maps. An empty instance defaults to one that cannot actually
    --   be mapped.
    class BlockStorable a => BlockMappable a where
        mapBlock :: MapOperation -> [Block a] -> Maybe (Block a)
        mapBlock _ blocks = error $ "mapBlock not implemented for type " ++ (show $ typeOf  (head blocks #! 0))

        isMappable :: Block a -> Bool
        isMappable _ = False