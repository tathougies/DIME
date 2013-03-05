{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Language.Flow.Lambda.Enriched
    (
     Expression(..),
     Program(..),
     Pattern(..),
     compileProgram
    ) where

import Control.Monad
import Control.Monad.State

import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Int
import Data.String
import Data.Typeable

import Language.Flow.Execution.Types hiding (Ap, IntConstant, StringConstant, DoubleConstant)
import Language.Flow.Execution.GMachine (newGMachineStateWithGCodeProgram, runGMachine, continue)

import System.IO.Unsafe

data Pattern = PlaceholderPattern |
               ConstrPattern T.Text [VarID]
  deriving (Show, Eq)

data Expression = IntConstant Int64 |
                  StringConstant String |
                  DoubleConstant Double |
                  Variable VarID |
                  SCRef SCName |
                  LocationRef GMachineAddress |
                  Ap Expression Expression |
                  Lambda [VarID] Expression |
                  Let [(VarID,Expression)] Expression |
                  Case VarID [(Pattern, Expression)] |
                  SimpleLet (VarID, Expression) Expression -- Only used internally
  deriving (Show, Eq)

data SuperCombinator = SuperCombinator [VarID] Expression
  deriving (Show, Eq)

data Program = Program {
      programSupercombinators :: Map.Map SCName SuperCombinator,
      programBody :: Expression
    }
  deriving (Show, Eq)

collectFreeVariables :: [VarID] -> Expression -> [VarID]
collectFreeVariables alreadyBound expr = Set.toList $ collectFreeVariables' (Set.fromList alreadyBound) expr
    where
      collectFreeVariables' :: Set.Set VarID -> Expression -> Set.Set VarID
      collectFreeVariables' alreadyBound (Variable x) = if Set.member x alreadyBound then Set.empty else Set.singleton x
      collectFreeVariables' alreadyBound (Ap e1 e2) =
          let freeE1 = collectFreeVariables' alreadyBound e1
              freeE2 = collectFreeVariables' alreadyBound e2
          in
            freeE1 `Set.union` freeE2
      collectFreeVariables' alreadyBound (Lambda varIds e) = collectFreeVariables' (alreadyBound `Set.union` (Set.fromList varIds)) e
      collectFreeVariables' alreadyBound (Let bindings e) =
          let exprs = map snd bindings
              bindingVars = map fst bindings
              freeInBindings = foldl Set.union Set.empty $
                               map (collectFreeVariables' alreadyBound) exprs
              freeInExpr = collectFreeVariables' (alreadyBound `Set.union` (Set.fromList bindingVars)) e
          in
            freeInBindings `Set.union` freeInExpr
      collectFreeVariables' alreadyBound _ = Set.empty

-- Lifts all lambda expressions
liftLambdas :: Expression -> State Program Expression
liftLambdas (Ap e1 e2) = do
  e1' <- liftLambdas e1
  e2' <- liftLambdas e2
  return $ Ap e1' e2'
liftLambdas (Let bindings expression) =
  do
    bindings' <- forM bindings $
                 (\(n, e) -> do
                    e' <- liftLambdas e
                    return (n, e'))
    expression' <- liftLambdas expression
    return $ Let bindings' expression'
liftLambdas (SimpleLet (v, b) e) = do
    b' <- liftLambdas b
    e' <- liftLambdas e
    return $ SimpleLet (v, b') e'
liftLambdas (Lambda varIds expression) = do
  expression' <- liftLambdas expression

  -- Now find all free variables in the expression, these will be passed in
  let freeVariables = collectFreeVariables varIds expression' -- TODO these should be ordered based on "freed"ness -- see book

  scName <- newSCName
  let newSc = SuperCombinator (freeVariables ++ varIds) expression'

  addSC scName newSc

  -- Now build the appropriate Ap chain
  return $ foldl Ap (SCRef scName) $ map Variable freeVariables
liftLambdas x = return x -- everything else just stays as is!

newSCName :: State Program SCName
newSCName = do
  Program { programSupercombinators = scs } <- get
  let (max, _) = Map.findMax scs
  if Map.null scs then return $ SCName 0 else
      return $ max + (SCName 1)

addSC :: SCName -> SuperCombinator -> State Program ()
addSC scName sc =
  modify (\program ->
              program { programSupercombinators =
                            Map.insert scName sc $ programSupercombinators program })

compileProgram :: GMachineAddress -> Program -> GCodeProgram
compileProgram scBase program =
    let body = programBody program
        (liftedBody, program') = runState (liftLambdas body) program
        (bodySC, program'') = runState (do
                                         bodySC <- newSCName
                                         addSC bodySC (SuperCombinator [] liftedBody)
                                         return bodySC) program'
        finalSCS = programSupercombinators program''

        labelledCompiledSCS = Map.assocs $ Map.mapKeysMonotonic (fromIntegral.(+(fromIntegral scBase))) $ Map.map (compileSC scBase) finalSCS
    in
      GCodeProgram { initCode = [PushLocation $ fromIntegral bodySC + fromIntegral scBase, Eval, ProgramDone],
                     initialData = labelledCompiledSCS }

    where
      compileSC :: GMachineAddress -> SuperCombinator -> GenericGData
      compileSC scBase (SuperCombinator args expr) =
          let arity = length args
              argsMap = Map.fromList $ zip args (map (arity -) [0..arity - 1])
          in
            mkGeneric $ Fun arity $ compileExpr argsMap arity scBase expr

      compileExpr :: Map.Map VarID Int -> Int -> GMachineAddress -> Expression -> GCodeSequence
      compileExpr argMap stackDepth scBase expr = (compileSubExpr argMap stackDepth scBase expr) ++
                                                  [Update $ StackOffset stackDepth + 1,
                                                   Pop $ fromIntegral stackDepth,
                                                   Unwind]

      compileSubExpr :: Map.Map VarID Int -> Int -> GMachineAddress -> Expression -> GCodeSequence
      compileSubExpr _ _ _ (IntConstant i) = [PushInt i]
      compileSubExpr _ _ _ (StringConstant s) = [PushString $ fromString s]
      compileSubExpr _ _ _ (DoubleConstant d) = [PushDouble d]
      compileSubExpr _ _ scBase (SCRef scName) = [PushLocation $ fromIntegral scName + fromIntegral scBase]
      compileSubExpr _ _ _ (LocationRef l) = [PushLocation l]
      compileSubExpr argMap stackDepth _ (Variable varId) =
          let Just argLocation = Map.lookup varId argMap
          in [Push $ StackOffset $ (fromIntegral stackDepth) - argLocation]
      compileSubExpr argMap stackDepth scBase (Ap e1 e2) =
          let compiled1 = compileSubExpr argMap (stackDepth + 1) scBase e1
              compiled2 = compileSubExpr argMap stackDepth scBase e2
          in compiled2 ++ compiled1 ++ [MkAp]
      compileSubExpr argMap stackDepth scBase (SimpleLet (varName, binding) e) =
          let compiledBinding = compileSubExpr argMap' stackDepth' scBase binding
              compiledExpr = compileSubExpr argMap' stackDepth' scBase e
              argMap' = Map.insert varName (stackDepth + 1) argMap
              stackDepth' = stackDepth + 1
          in compiledBinding ++ compiledExpr ++ [Slide $ StackOffset 1]
      compileSubExpr argMap stackDepth scBase (Case varName patsAndExprs) =
          let compiledExprs = map compileAlternative patsAndExprs -- Compile the individual alternatives

              compileAlternative (PlaceholderPattern, expr) = compileSubExpr argMap stackDepth scBase expr -- Nothing will be pushed onto the stack
              compileAlternative (ConstrPattern tName varIds, expr) = -- In this case, the arguments to the constructor will be pushed onto the stack
                  let arity = length varIds
                      stackDepth' = stackDepth + arity
                      argMapPart = Map.fromList $ zip varIds $ map (stackDepth' - ) [0..arity - 1]
                      argMapBase = Map.map (+ arity) argMap
                      argMap' = argMapBase `Map.union` argMapPart
                  in compileSubExpr argMap' stackDepth' scBase expr

              compiledExprLengths = (map ((+1).length) $ init compiledExprs) ++ [length $ last compiledExprs] -- Add 1 to account for the jump at the end of each expression, except the last
              compiledExprAccumLengths = init $ scanl (+) 0 compiledExprLengths -- The offset from the top of the alternative block to find each alternative
              jumpOffsetsFromAltStart = map (+1) compiledExprAccumLengths -- Shift by 1 to account for the extra pop jump instruction at the end

              alternativeCount = length patsAndExprs -- The first alternative is alternativeCount - 1 instructions away from the top
                                                     -- The next is alternativeCount - 2, and so on
              offsetToAltStart = zipWith (-) (repeat alternativeCount) [1..alternativeCount] -- Calculate offset from each alternative's location
                                                                                          -- to the start of the alternate block

              finalOffsets = zipWith (+) jumpOffsetsFromAltStart offsetToAltStart -- Add together the two offsets

              compilePat (PlaceholderPattern, _) offs = [Pop $ StackOffset 1, Jump (offs - 1)]
              compilePat (ConstrPattern tName varIds, _) offs = [Examine tName (length varIds) offs]

              compiledPats = concatMap (uncurry compilePat) $ zip patsAndExprs finalOffsets

              -- Now calculate the offset needed to jump past the expression block for each alternative
              -- Alternative 1 needs to jump the length of all alternatives > 1
              jumpOffsets = reverse (tail $ scanl (+) 0 (reverse $ tail compiledExprLengths))
              jumpInstrs = map Jump jumpOffsets

              alternateBlock = (concatMap (\(expr, jump) -> expr ++ [jump]) $ zip compiledExprs jumpInstrs) ++ last compiledExprs

              Just argLocation = Map.lookup varName argMap
              examineVar = StackOffset $ (fromIntegral stackDepth) - argLocation

          in [Push examineVar] ++ compiledPats ++ alternateBlock
      compileSubExpr argMap stackDepth scBase (Let bindings e) =
          let bindingCount = length bindings
              stackDepth' = stackDepth + bindingCount
              locations = Map.fromList $ zip (map fst bindings) [stackDepth + 1..stackDepth+bindingCount]
              argMap' = argMap `Map.union` locations

              letrecExpressions = cLetrec argMap' stackDepth' scBase bindings
              body = compileSubExpr argMap' stackDepth' scBase e
          in
            letrecExpressions ++ body ++ [Slide $ StackOffset $ stackDepth' - stackDepth]

      cLetrec :: Map.Map VarID Int -> Int -> GMachineAddress -> [(VarID, Expression)] -> GCodeSequence
      cLetrec argMap stackDepth scBase bindings =
          let bindingCount = length bindings
              compilations = map (compileSubExpr argMap stackDepth scBase) $ map snd bindings
              finalCompilations = map (\(i, x) -> x ++ [Update $ StackOffset $ bindingCount - i]) $
                                  zip [0..bindingCount - 1] compilations
          in (Alloc bindingCount) : (concat finalCompilations)

newGMachineWithLambda :: Typeable a => Int -> a -> [(GMachineAddress, GenericGData)] -> Expression -> IO GMachineState
newGMachineWithLambda cells userState initData e =
    let (e', program) = runState (liftLambdas e) (Program { programSupercombinators = Map.empty,
                                                            programBody = undefined })
        program' = program { programBody = e' }
        gcodeProgram = compileProgram (maximum $ map fst initData) program'
    in
      newGMachineStateWithGCodeProgram cells userState initData gcodeProgram

runLambda :: Typeable a => Int -> a -> [(GMachineAddress, GenericGData)] -> Expression -> IO GenericGData
runLambda cells userState initData e = do
  gmachine <- newGMachineWithLambda cells userState initData e
  res <- runGMachine continue gmachine
  case res of
    Left (GMachineError _ e) -> fail $ "GMachine failed " ++ show e
    Right (_, d) -> return d