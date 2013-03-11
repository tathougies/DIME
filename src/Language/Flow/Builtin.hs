{-# LANGUAGE RankNTypes, ViewPatterns, OverloadedStrings #-}
module Language.Flow.Builtin where

import Control.Monad

import qualified Data.Text as T
import Data.List
import Data.String
import Data.Array

import Language.Flow.Execution.Types
import Language.Flow.Execution.GMachine
import Language.Flow.Module

headwater :: Module -- The equivalent of the Haskell Prelude in Flow
headwater = mkModule "HeadWater"
            [("(+)", mkBuiltin "(+)" 2 builtinAdd),
             ("(-)", mkBuiltin "(-)" 2 $ builtinBinOp "subtract" (-)),
             ("(*)", mkBuiltin "(*)" 2 builtinMul),
             ("(/)", mkBuiltin "(/)" 2 builtinDiv),
             ("(**)", mkBuiltin "(**)" 2 builtinPow),
             ("negate", mkBuiltin "negate" 1 builtinNegate),
             ("if", mkBuiltin "if" 3 builtinIf),
             ("error", mkBuiltin "error" 1 builtinError),
             ("mkAccessor", mkBuiltin "mkAccessor" 2 builtinMkAccessor),
             ("Cons", mkBuiltin' "Cons" 2 builtinCons),
             ("Null", mkGeneric $ GNull),
             ("True", mkGeneric $ GTrue),
             ("False", mkGeneric$ GFalse)]
    where
      builtinBinOp :: String -> (forall a. Num a => a -> a -> a) -> [GenericGData] -> GMachine (Maybe GenericGData)
      builtinBinOp e f [x, y]
          | isInteger x && isInteger y = returnPureGeneric $ IntConstant $ f (asInteger x) (asInteger y)
          | isDouble x && isDouble y = returnPureGeneric $ DoubleConstant $ f (asDouble x) (asDouble y)
          | withGenericData typeName x == withGenericData typeName y && withGenericData supportsGeneric x e =
              withGenericData runGeneric x e [x,y] -- if both are the same type and this generic is supported, try it
          | otherwise = throwError $ "Cannot " ++ e ++ " " ++ (T.unpack $ withGenericData typeName x) ++ " and " ++ (T.unpack $ withGenericData typeName y) ++ " objects."
      builtinBinOp e _ _ = throwError $ "Can only " ++ e ++ " two values"

      builtinAdd [x, y]
          | isString x && isString y = returnPureGeneric $ StringConstant $ T.append (asString x) (asString y)
      builtinAdd x = builtinBinOp "add" (+) x

      builtinMul [x, y]
          | isString x && isInteger y = returnPureGeneric $ StringConstant $ T.replicate (fromIntegral $ asInteger y) (asString x)
          | isInteger x && isString y = builtinMul [y, x]
      builtinMul x = builtinBinOp "multiply" (*) x

      builtinDiv [x, y]
          | isInteger x && isInteger y = returnPureGeneric $ IntConstant $ (asInteger x) `div` (asInteger y)
      builtinDiv x = throwError $ "Cannot divide " ++ show x

      builtinPow [x, y]
          | isInteger x && isInteger y = returnPureGeneric $ IntConstant $ (asInteger x) ^ (asInteger y)
      builtinPow x = throwError $ "Cannot exponentiate " ++ show x

      builtinNegate [x]
          | isInteger x = returnPureGeneric $ IntConstant $ negate (asInteger x)
      builtinNegate x = throwError $ "Cannot negate " ++ show x

      builtinIf [x, y, z]
          | isBool x = case (asBool x) of
                         GTrue -> returnPure y
                         GFalse -> returnPure z
          | otherwise = throwError $ "Bool conditional expected in if"

      builtinError [x]
          | isString x = throwError $ T.unpack $ asString x
      builtinError x = throwError $ "Invalid call to error. Got arguments " ++ show x

      builtinCons [x, y] = (trace "Consing!") >> (returnPureGeneric $ GCons x y)
      builtinCons _ = throwError $ "Cons takes two arguments"

      builtinMkAccessor [field', o]
        | isString field' = let field = asString field'
                            in liftM Just $ withGenericData (getField field) o
--returnPureGeneric $ StringConstant $ fromString $ "Field " ++ show field ++ " of " ++ show o
      builtinMkAccessor [_, _] = throwError $ "mkAccessor field name must be string"

-- Data types
data GList = GCons {-# UNPACK #-} !GMachineAddress {-# UNPACK #-} !GMachineAddress |
             GNull
     deriving (Show, Eq)

isList :: GenericGData -> Bool
isList = checkType (typeName (undefined :: GList))

asList :: GenericGData -> GList
asList = checkCoerce (typeName (undefined :: GList))

constrList :: GConstr -> [GMachineAddress] -> GenericGData
constrList (T.unpack -> "Cons") [x, y] = mkGeneric $ GCons x y
constrList (T.unpack -> "[]") [] = mkGeneric GNull
constrList cName _ = error $ "Invalid constructor for List: " ++ T.unpack cName

instance GData GList where
    typeName _ = fromString "HeadWater.List"
    constr (GCons _ _) = fromString "Cons"
    constr GNull = fromString "[]"
    constrArgs (GCons a1 a2) = listArray (0,1) [a1, a2]
    constrArgs GNull = array (0, -1) []

-- | Given the address of the first list cell, reads a list of GenericGData
readGDataList :: GMachineAddress -> GMachine [GenericGData]
readGDataList a = do
        -- Evaluate the address first
        gEvaluate a
        datGeneric <- readGraph a
        if isList datGeneric then
            case asList datGeneric of
              GCons e1 e2 -> do
                    e1D <- readGraph e1
                    e2L <- readGDataList e2
                    return $ e1D : e2L
              GNull -> return []
         else throwError $ "Expecting list. Got " ++ show datGeneric

data GBool = GTrue | GFalse
             deriving (Show, Eq, Ord, Enum, Read)

isBool :: GenericGData -> Bool
isBool = checkType (typeName (undefined :: GBool))

asBool :: GenericGData -> GBool
asBool = checkCoerce (typeName (undefined :: GBool))

constrBool :: GConstr -> [GMachineAddress] -> GenericGData
constrBool (T.unpack -> "True") [] = mkGeneric GTrue
constrBool (T.unpack -> "False") [] = mkGeneric GFalse
constrBool _ _ = error "Invalid constructor for Bool"

instance GData GBool where
    typeName _ = fromString "HeadWater.Bool"
    constr GTrue = fromString "True"
    constr GFalse = fromString "False"
    constrArgs _ = array (0,-1) []

initFlowLanguageBuiltins :: IO ()
initFlowLanguageBuiltins = do
  registerBuiltinModule headwater
  registerGData (undefined :: GBool) constrBool
  registerGData (undefined :: GList) constrList