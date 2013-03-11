{-# LANGUAGE OverloadedStrings #-}
module Language.Flow.Module
    (Module(flowModuleMembers, flowModuleName),

     mkModule, mkBuiltin, mkBuiltin',
     returnPure, returnPureGeneric,
     returnGeneric)
        where

import Control.Monad.Trans
import Control.Monad.State

import qualified Data.Map as M
import Data.List (concat, replicate)
import Data.Text hiding (map, concat, replicate)
import Data.String

import Language.Flow.Execution.Types
import Language.Flow.Execution.GMachine
import Language.Flow.AST

mkModule :: String -> [(String, GenericGData)] -> Module
mkModule name members = let modName = fromString name
                            membersMap = M.fromList $ map (\(name, e) -> (fromString name, fixBuiltinFun e)) members

                            fixBuiltinFun e
                                | withGenericData isBuiltin e =
                                    case withGenericData asBuiltin e of
                                      BuiltinFun arity _ symName builtinFun -> mkGeneric $ BuiltinFun arity modName symName builtinFun
                                      _ -> e
                                | otherwise = e
                        in Module modName membersMap

-- | Create GenericGData Builtin function that takes GMachineAddress's as input (useful for data constructors, etc)
mkBuiltin' :: VariableName -> Int -> ([GMachineAddress] -> GMachine (Maybe GenericGData)) -> GenericGData
mkBuiltin' symName arity baseFn = mkGeneric $ BuiltinFun arity "" symName (GCodeBuiltin baseFn)

-- | Create GenericGData Builtin that is strict in all its arguments... this is most builtin functions
mkBuiltin :: VariableName -> Int -> ([GenericGData] -> GMachine (Maybe GenericGData)) -> GenericGData
mkBuiltin symName arity baseFn =
    mkGeneric $ BuiltinFun arity "" symName $ GCodeBuiltin $
                  (\addrs -> do
                     -- evaluate all addresses
                     let instrs' = concat $ replicate arity [
                                    Push $ StackOffset $ arity - 1,
                                    Eval]
                         instrs = instrs' ++ [ProgramDone]

                     pushDump addrs instrs

                     -- Evaluate arguments
                     continue

                     -- state <- get

                     -- -- Evaluate arguments
                     -- res <- liftIO $ runGMachine continue state
                     -- newState <- case res of
                     --               Left e -> throwError' e
                     --               Right (state', _) -> return state'
                     -- put newState

                     addrs' <- replicateM arity popStack
                     args <- mapM readGraph addrs'

                     popDump

                     baseFn args)

returnPure :: GenericGData -> GMachine (Maybe GenericGData)
returnPure = return . Just

returnPureGeneric :: GData a => a -> GMachine (Maybe GenericGData)
returnPureGeneric = returnPure . mkGeneric

returnGeneric :: GData a => a -> GMachine GenericGData
returnGeneric = return . mkGeneric