module Language.Flow.Module
    (Module(flowModuleMembers, flowModuleName),

     mkModule, mkBuiltin,
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

data Module = Module {
            flowModuleName :: ModuleName,
            flowModuleMembers :: M.Map VariableName GenericGData
    }

mkModule :: String -> [(String, GenericGData)] -> Module
mkModule name members = let modName = fromString name
                            membersMap = M.fromList $ map (\(name, e) -> (fromString name, e)) members
                        in Module modName membersMap

mkBuiltin :: Int -> ([GenericGData] -> GMachine (Maybe GenericGData)) -> GenericGData
mkBuiltin arity baseFn = mkGeneric $ BuiltinFun arity $ GCodeBuiltin $
                         (\addrs -> do
                            -- evaluate all addresses
                            let instrs' = concat $ replicate arity [
                                           Push $ StackOffset $ arity - 1,
                                           Eval]
                                instrs = instrs' ++ [ProgramDone]

                            pushDump addrs instrs

                            state <- get

                            -- Evaluate arguments
                            res <- liftIO $ runGMachine continue state
                            newState <- case res of
                                          Left e -> throwError' e
                                          Right (state', _) -> return state'
                            put newState

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