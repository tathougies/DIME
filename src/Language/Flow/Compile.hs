{-# LANGUAGE OverloadedStrings #-}
module Language.Flow.Compile
    (runProgramFromString,
     runProgramFromText,
     compileProgramFromString,
     compileProgramFromText,
     compileProgram)
        where

import Control.Monad
import Control.Monad.Trans
import Control.Monad.State
import Control.Exception

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.List as List
import Data.List (sortBy, groupBy)
import Data.Char
import Data.String
import Data.Maybe
import Data.Function
import Data.IORef
import Data.Either
import Data.Typeable
import Data.Foldable (foldrM)

import qualified Language.Flow.AST as L
import qualified Language.Flow.Lambda.Enriched as E
import qualified Language.Flow.Builtin as FlowBuiltin
import Language.Flow.Execution.Types hiding (Ap)
import Language.Flow.Parse
import Language.Flow.Module
import Language.Flow.Execution.GMachine

import System.IO.Unsafe
import System.Log.Logger

import Text.Parsec (sourceName, sourceLine, sourceColumn)

moduleName = "Language.Flow.Compile"

data FlowCompilerState = State {
      moduleMap :: M.Map ModuleName Module, -- map module names to module objects
      globalModules :: [ModuleName], -- modules in the global namespace

      referencedSymbols :: S.Set (ModuleName, L.VariableName), -- Set of referenced symbols

      symbolMap :: M.Map (ModuleName, L.VariableName) GMachineAddress, -- Map of symbols to pseud-addresses
      curAddress :: GMachineAddress,

      curVarId :: VarID
    }

type FlowCompiler = StateT FlowCompilerState IO

emptyState = State {
               moduleMap = M.empty,
               globalModules = [],
               referencedSymbols = S.empty,
               symbolMap = M.empty,
               curAddress = fromIntegral 0,
               curVarId = 0
             }

compileError (L.Region start end) error =
    let sourceFile = sourceName start
        startLine = sourceLine start
        startChar = sourceColumn start
        endLine = sourceLine end
        endChar = sourceColumn end

        startPosStr = show startLine ++ ":" ++ show startChar
        endPosStr = show endLine ++ ":" ++ show endChar
    in
      fail $ "At " ++ sourceFile ++ " " ++ startPosStr ++ "--" ++ endPosStr ++ ": " ++ error

allocNextSymbolAddress :: FlowCompiler GMachineAddress
allocNextSymbolAddress = do
  st <- get
  put $ st { curAddress = 1 + curAddress st }
  return $ curAddress st

allocNextVarId :: FlowCompiler VarID
allocNextVarId = do
  st <- get
  put $ st { curVarId = 1 + curVarId st }
  return $ curVarId st

getSymbolAddress :: (ModuleName, L.VariableName) -> FlowCompiler GMachineAddress
getSymbolAddress symbol = do
  symMap <- liftM symbolMap get
  case M.lookup symbol symMap of
    Just a -> return a
    Nothing -> do
      -- allocate new address
      newAddr <- allocNextSymbolAddress
      modify (\st -> st { symbolMap = M.insert symbol newAddr symMap })
      return newAddr

runProgramFromString :: Typeable a => Int -> a -> String -> IO (GMachineState, GenericGData)
runProgramFromString cells userState progStr = runProgramFromText cells userState $ fromString progStr

runProgramFromText :: Typeable a => Int -> a -> T.Text -> IO (GMachineState, GenericGData)
runProgramFromText cells userState progTxt = do
  prog <- compileProgramFromText progTxt
  gm <- newGMachineStateWithGCodeProgram cells userState [] prog
  res <- runGMachine continue gm
  case res of
    Left exc -> throwIO exc
    Right x -> return x

compileProgramFromString :: String -> IO GCodeProgram
compileProgramFromString progStr = compileProgramFromText $ fromString progStr

compileProgramFromText :: T.Text -> IO GCodeProgram
compileProgramFromText progTxt = do
  prog <- parseProgram "<compileProgramFromString>" progTxt
  compileProgram prog

compileProgram :: L.Program -> IO GCodeProgram
compileProgram program = liftM fst $ runStateT action emptyState
    where
      action = do
        -- We will now convert the full flow AST into the enriched lambda calculus
        -- In doing so we will place symbols at specific locations in the G-Machine's memory
        -- This data will be encapsulated in the returned GCodeProgram object

        -- The following steps must be undertaken to do the conversion
        --     * Patterns must be lifted
        --     * Identifiers must be resolved
        --     * Type assertions must be transformed
        --     * Patterns in let expressions must be transformed into simple patterns
        --     * Lets must be transformed into series of lets and letrecs
        loadAllImports program

        liftIO $ infoM moduleName "Going to compile:"
        liftIO $ infoM moduleName $ L.prettyPrintAST $ L.flowProgramBody program

        resolvedBody <- resolveIdentifiers $ L.flowProgramBody program
        liftIO $ infoM moduleName "\nAfter identifiers were resolved:"
        liftIO $ infoM moduleName $ L.prettyPrintAST $ resolvedBody

        simplifiedPatternsBody <- simplifyFunctionPatterns resolvedBody

        let body' = simplifiedPatternsBody

        liftIO $ infoM moduleName "\nAfter simplifying patterns:"
        liftIO $ infoM moduleName $ L.prettyPrintAST $ body'

        -- Convert the AST into the enriched lambda calculus
        let enrichedLambdaE = lowerASTIntoEnrichedLambdaCalculus body'
        liftIO $ infoM moduleName $ "\nAnd the lowered lambda:"
        liftIO $ infoM moduleName $ show enrichedLambdaE

        -- Create the initial data that we'll need
        st <- get
        let symbolsAndAddresses = M.assocs $ symbolMap st
            lookupSymbol (modName, name) =
                let Just mod = M.lookup modName $ moduleMap st
                    Just member = M.lookup name $ flowModuleMembers mod
                in member
            initData = map (\(symbol, addr) -> (addr, lookupSymbol symbol)) symbolsAndAddresses

            program = E.Program { E.programSupercombinators = M.empty, E.programBody = enrichedLambdaE }
            gcodeProgram = E.compileProgram (curAddress st) program

        liftIO $ infoM moduleName $ "\nAnd the compiled gcode"
        liftIO $ infoM moduleName $ show gcodeProgram

        return $ gcodeProgram { initialData = initData ++ initialData gcodeProgram }

loadAllImports :: L.Program -> FlowCompiler ()
loadAllImports program = do
  let imports = L.flowProgramImports program -- get names of imports

  mapM_ (uncurry loadModule) imports

  modify (\st -> st { globalModules = L.flowGlobalImports program })

loadModule :: ModuleName -> ModuleName -> FlowCompiler ()
loadModule moduleName modulePseudonym = do
  bm <- liftIO $ getBuiltinModules
  case M.lookup moduleName bm of -- check builtin modules first
    Just mod -> addModule modulePseudonym mod -- simply add the module to the list
    Nothing -> fail $ "need to add support for loading modules from disk: " ++ show moduleName

addModule :: ModuleName -> Module -> FlowCompiler ()
addModule moduleName mod = modify (\st -> st { moduleMap = M.insert moduleName mod $ moduleMap st })

formatRegion :: L.Region -> String
formatRegion (L.Region start end) =
    let fileName = sourceName start
        startR = (show $ sourceLine start) ++ ":" ++ (show $ sourceColumn start)
        endR = (show $ sourceLine end) ++ ":" ++ (show $ sourceColumn end)
    in fileName ++ " (" ++ startR ++ " - " ++ endR ++ ")"

mkErrorCall :: String -> FlowCompiler L.Expression
mkErrorCall e = do
  Just (Right symbol) <- lookupIdentifier M.empty (fromString "error")
  referenceSymbol symbol
  symbolAddress <- getSymbolAddress symbol
  return $ L.Ap L.EmptyRegion
             (L.LocationRef L.EmptyRegion symbolAddress)
             (L.Literal L.EmptyRegion $ L.StringLiteral $ fromString e)

lookupIdentifier :: M.Map L.VariableName VarID -> L.VariableName -> FlowCompiler (Maybe (Either VarID (ModuleName, L.VariableName)))
lookupIdentifier scope name = do
  let L.VariableName nameText = name
  if isJust $ T.find (=='.') nameText then do
      -- Fully qualified name
      let components = T.split (=='.') nameText
          modName = ModuleName $ T.intercalate (fromString ".") $ List.init components
          member = L.VariableName $ List.last components
      modules <- liftM moduleMap get
      case M.lookup modName modules of
        Nothing -> return Nothing -- Cannot find module
        Just mod -> let modMembers = flowModuleMembers mod
                    in case M.lookup member modMembers of
                         Just _ -> return $ Just $ Right (modName, member)
                         Nothing -> return Nothing -- cannot find member
   else do
     -- look up in scope
     let scopeRes = M.lookup name scope
     case scopeRes of
       Just x -> return $ Just $ Left x
       Nothing -> do -- look in global modules
          modMap <- liftM moduleMap get
          let lookupGlobal _ [] = return Nothing
              lookupGlobal name (modName:modNames) = do
                     let Just mod = M.lookup modName modMap
                         modMembers = flowModuleMembers mod
                     case M.lookup name modMembers of
                       Just _ -> return $ Just $ Right (modName, name)
                       Nothing -> lookupGlobal name modNames

          globalMods <- liftM globalModules get
          lookupGlobal name globalMods

referenceSymbol :: (ModuleName, L.VariableName) -> FlowCompiler ()
referenceSymbol symbol = modify (\st -> st { referencedSymbols = S.insert symbol $ referencedSymbols st })

resolveIdentifiers :: L.Expression -> FlowCompiler L.Expression
resolveIdentifiers expr = resolveIdentifiers' M.empty expr
    where
      resolveIdentifiers' :: M.Map L.VariableName VarID -> L.Expression -> FlowCompiler L.Expression
      resolveIdentifiers' scope (L.Identifier r name) = do
        symbol <- lookupIdentifier scope name -- looks up an identifier, returns (module, name) pair
        case symbol of
          Nothing -> compileError r $ "Could not resolve identifier " ++ show name
          Just (Left varId) -> return $ L.VariableRef r varId
          Just (Right symbol) -> do
                      referenceSymbol symbol
                      symbolAddress <- getSymbolAddress symbol
                      return $ L.LocationRef r symbolAddress
      resolveIdentifiers' scope (L.Ap r f x) = do
          f' <- resolveIdentifiers' scope f
          x' <- resolveIdentifiers' scope x
          return $ L.Ap r f' x'
      resolveIdentifiers' scope (L.LetIn r typeAssertions bindings e) = do
          let varNames = map (\(L.Binding _ (L.VariablePattern _ x) _) -> x) bindings
              varRegions = map (\(L.Binding _ (L.VariablePattern r _) _) -> r) bindings

              uniqueNames = List.nub $ List.sort $ varNames
          varIds <- replicateM (length uniqueNames) allocNextVarId
          let newVars = M.fromList $ zip uniqueNames varIds
              scope' = newVars `M.union` scope
              lookupVarId name = fromJust $ M.lookup name newVars

          bindings' <- forM (zip3 varNames bindings varRegions) $
                       (\(varName, L.Binding r' _ bindingExpr, varRegion) -> liftM (L.Binding r' (L.VarIDPattern varRegion $ lookupVarId varName)) (resolveIdentifiers' scope' bindingExpr))
          typeAssertions' <- forM typeAssertions $
                             (\(L.TypeAssertion name t) -> do
                                  let res = M.lookup name scope'
                                  case res of
                                    Just varId -> return $ L.TypeAssertionId varId t
                                    Nothing -> compileError r $ "Could not resolve identifier " ++ show name)
          e' <- resolveIdentifiers' scope' e
          return $ L.LetIn r typeAssertions' bindings' e'
      resolveIdentifiers' scope (L.Case r e patsAndExprs) = do
        e' <- resolveIdentifiers' scope e
        patsAndExprs' <- mapM (\(pat, expr) -> do
                                (pat', scope') <- runStateT (resolveIdentifiersPat pat) scope
                                expr' <- resolveIdentifiers' scope' expr
                                return (pat', expr')) patsAndExprs
        return $ L.Case r e' patsAndExprs'
      resolveIdentifiers' scope (L.TypeAssertionE r e t) = do
          e' <- resolveIdentifiers' scope e
          return $ L.TypeAssertionE r e' t
      resolveIdentifiers' scope (L.Block r es) = liftM (L.Block r) $ mapM (resolveIdentifiers' scope) es
      resolveIdentifiers' scope (L.Lambda r patterns e) = do
        -- let pats = map (\pat -> case pat of
        --                           L.VariablePattern r x -> Left $ (r, x)
        --                           pat' -> Right pat') patterns
        --     varPatsOnly = lefts pats
        --     varNames = map snd varPatsOnly
        --     varRegions = map fst varPatsOnly
        -- varIds <- replicateM (length varNames) allocNextVarId
        -- let newVars = M.fromList $ zip varNames varIds
        --     scope' = newVars `M.union` scope
        --     varIdPatterns = map (\(id, r) -> L.VarIDPattern r id) $ zip varIds varRegions

        --     patterns' = replaceLefts pats varIdPatterns

        --     replaceLefts ((Left _):xs) (left:lefts) = left:(replaceLefts xs lefts)
        --     replaceLefts ((Right x):xs) lefts = x:(replaceLefts xs lefts)
        --     replaceLefts [] [] = []

        (patterns', scope') <- runStateT (mapM resolveIdentifiersPat patterns) scope

        e' <- resolveIdentifiers' scope' e
        return $ L.Lambda r patterns' e'
      resolveIdentifiers' _ x = return x

      resolveIdentifiersPat :: L.Pattern -> StateT (M.Map L.VariableName VarID) FlowCompiler L.Pattern
      resolveIdentifiersPat (L.VariablePattern r vName) = do
        varId <- lift $ allocNextVarId
        modify (\scope -> M.insert vName varId scope)
        return $ L.VarIDPattern r varId
      resolveIdentifiersPat (L.ConstrPattern r typeName pats) = do
        pats' <- mapM resolveIdentifiersPat pats
        return $ L.ConstrPattern r typeName pats'
      resolveIdentifiersPat x = return x

simplifyFunctionPatterns :: L.Expression -> FlowCompiler L.Expression
simplifyFunctionPatterns (L.Lambda r patterns expression) = do
  e' <- simplifyFunctionPatterns expression
  return $ L.Lambda r patterns e'
simplifyFunctionPatterns (L.LetIn r assertions bindings e) = do
  e' <- simplifyFunctionPatterns e
  -- Group similar bindings together
  let (constrBindings, variableBindings) = List.partition (\x -> case x of
                                                                  L.Binding _ (L.ConstrPattern _ _ _) _ -> True
                                                                  _ -> False) bindings


      variableBindings' = map (\(L.Binding _ (L.VarIDPattern _ v) e) -> (v, e)) variableBindings

      groupedBindings = groupBy ((==) `on` fst) $ sortBy (compare `on` fst) variableBindings'

      -- if there's more than one definition, and it's not a lambda expression, that's an error
      -- if there's more than one defintion and lambda pattern counts don't match, that's an error

  groupedBindings' <- forM groupedBindings $
      (\varBindings@((v, e):xs) ->
              case xs of
                [] -> case e of
                        L.Lambda lambdaR pats e -> do
                               let arity = length pats

                               varIds <- replicateM arity allocNextVarId

                               let patternsAndExpressions = [(pats, e)]

                               errCall <- mkErrorCall $ "Incomplete pattern specification at " ++ formatRegion lambdaR
                               newExpr <- compilePattern varIds patternsAndExpressions errCall

                               newExpr' <- simplifyFunctionPatterns newExpr

                               let newLambda = L.Lambda lambdaR newLambdaPatterns newExpr'
                                   newLambdaPatterns = map (\varId -> L.VarIDPattern r varId) varIds

                               return $ L.Binding (L.exprRegion e) (L.VarIDPattern (L.exprRegion e) v) newLambda
                        _ -> do
                          e' <- simplifyFunctionPatterns e
                          return $ L.Binding (L.exprRegion e) (L.VarIDPattern (L.exprRegion e) v) e'
                _ -> case e of
                       L.Lambda _ pats _ ->
                           let arityCheck = all ((== expectedArity) . lambdaArity . snd) varBindings

                               expectedArity = length pats
                               lambdaArity (L.Lambda _ ps _) = length ps

                               compiledPatterns = do
                                 varIds <- replicateM expectedArity allocNextVarId
                                 let patternsAndExpressions = map (\(_, (L.Lambda _ ps e)) -> (ps, e)) varBindings
                                 errCall <- mkErrorCall $ "Incomplete pattern specification at " ++ formatRegion r
                                 newExpr <- compilePattern varIds patternsAndExpressions errCall
                                 newExpr' <- simplifyFunctionPatterns newExpr

                                 -- now make a lambda expression and call it
                                 let newLambda = L.Lambda (L.exprRegion e) newLambdaPatterns newExpr'
                                     newLambdaPatterns = map (\varId -> L.VarIDPattern r varId) varIds
                                 return $ L.Binding (L.exprRegion e) (L.VarIDPattern (L.exprRegion e) v) newLambda

                           in if arityCheck then compiledPatterns else
                                  compileError r $ "Definitions for " ++ show v ++ " have different number of arguments"
                       _ -> compileError r $ "Redefinition of variable " ++ show v -- multiple definitions, the expression must be a lambda one
      )

  return $ L.LetIn r assertions groupedBindings' e'
simplifyFunctionPatterns (L.Ap r f x) = liftM2 (L.Ap r) (simplifyFunctionPatterns f) (simplifyFunctionPatterns x)
simplifyFunctionPatterns (L.Block r exprs) = liftM (L.Block r) $ mapM simplifyFunctionPatterns exprs
simplifyFunctionPatterns (L.TypeAssertionE r expr t) = do
  expr' <- simplifyFunctionPatterns expr
  return $ L.TypeAssertionE r expr' t
simplifyFunctionPatterns x = return x

lowerASTIntoEnrichedLambdaCalculus :: L.Expression -> E.Expression
lowerASTIntoEnrichedLambdaCalculus (L.LocationRef _ a) = E.LocationRef a
lowerASTIntoEnrichedLambdaCalculus (L.VariableRef _ a) = E.Variable a
lowerASTIntoEnrichedLambdaCalculus (L.Lambda _ patterns expression) =
    let arguments = map (\(L.VarIDPattern _ x) -> x) patterns
    in E.Lambda arguments $ lowerASTIntoEnrichedLambdaCalculus expression
lowerASTIntoEnrichedLambdaCalculus (L.LetIn _ [] [] e) = lowerASTIntoEnrichedLambdaCalculus e
lowerASTIntoEnrichedLambdaCalculus (L.LetIn _ [] [L.Binding _ (L.VarIDPattern _ v) e] expression) =
    E.SimpleLet (v, lowerASTIntoEnrichedLambdaCalculus e) $ lowerASTIntoEnrichedLambdaCalculus expression
lowerASTIntoEnrichedLambdaCalculus (L.LetIn _ [] bindings expression) =
    let bindings' = map (\(L.Binding _ (L.VarIDPattern _ v) e) -> (v, lowerASTIntoEnrichedLambdaCalculus e)) bindings
        expression' = lowerASTIntoEnrichedLambdaCalculus expression
    in E.Let bindings' expression'
lowerASTIntoEnrichedLambdaCalculus (L.Ap _ f x) = E.Ap (lowerASTIntoEnrichedLambdaCalculus f) (lowerASTIntoEnrichedLambdaCalculus x)
lowerASTIntoEnrichedLambdaCalculus (L.Literal _ (L.IntLiteral i)) = E.IntConstant i
lowerASTIntoEnrichedLambdaCalculus (L.Literal _ (L.StringLiteral s)) = E.StringConstant s
lowerASTIntoEnrichedLambdaCalculus (L.Literal _ (L.DoubleLiteral d)) = E.DoubleConstant d
lowerASTIntoEnrichedLambdaCalculus (L.Case _ (L.VariableRef _ varId) patsAndExprs) =
    let patsAndExprs' = map (\(pat, expr) -> (lowerPat pat, lowerASTIntoEnrichedLambdaCalculus expr)) patsAndExprs
        lowerPat (L.PlaceholderPattern _) = E.PlaceholderPattern
        lowerPat (L.ConstrPattern _ (L.TypeName tName) pats) = E.ConstrPattern tName $ map (\(L.VarIDPattern _ vId) -> vId) pats
    in E.Case varId patsAndExprs'

-- Pattern compiler. TODO debug this!
compilePattern :: [VarID] -> [([L.Pattern], L.Expression)] -> L.Expression -> FlowCompiler L.Expression
compilePattern [] [([], e)] _ = return e
compilePattern [] xs _ = compileError (L.exprRegion $ snd $ head xs) $ "Overlapping patterns"
compilePattern arguments patternsAndExpressions defaultExpression
    | beginsInAllVariables patternsAndExpressions =
        let patternsAndExpressions' = map (\(pat, expr) -> (tail pat,
                                                            replaceVar (getVarIDFromPattern $ head pat)
                                                              (head arguments)
                                                              expr))
                                      patternsAndExpressions
        in compilePattern (tail arguments) patternsAndExpressions' defaultExpression
    | beginsInAllConstructors patternsAndExpressions = do
        let sortedPatternsAndExpressions = groupBy ((==) `on` typeName) $ sortBy (compare `on` typeName) patternsAndExpressions
            typeName (((L.ConstrPattern _ t _):_), _) = t
        patternsAndExpressions' <- forM sortedPatternsAndExpressions $
                                    (\patExprs@(((L.ConstrPattern r typeName cArguments):_, exp):_) -> do
                                        varIds <- replicateM (length cArguments) allocNextVarId
                                        let varIdPatterns = map (L.VarIDPattern r) varIds
                                            pattern' = L.ConstrPattern r typeName varIdPatterns
                                        constrPatterns <- forM patExprs $
                                                          (\(pats, constrExp) ->
                                                             let constrVars' = varIdPatterns ++ tail pats
                                                             in return $ (constrVars', constrExp))
                                        exp' <- compilePattern (varIds ++ tail arguments) constrPatterns defaultExpression
                                        return $ (pattern', exp')
                                     )
        let (_, e) = head patternsAndExpressions
            patternsAndExpressions'' = patternsAndExpressions' ++ [(L.PlaceholderPattern $ L.exprRegion e, defaultExpression)]
        return $ L.Case (L.exprRegion e) (L.VariableRef L.EmptyRegion $ head arguments) patternsAndExpressions''
  -- constructor rule
    | otherwise = let groupedByVariable = groupBy ((==) `on` isVariable) patternsAndExpressions -- mixture rule
                  in foldrM (compilePattern arguments) defaultExpression groupedByVariable
    where
      isVariable ((L.VarIDPattern _ _):_, _) = True
      isVariable ((L.ConstrPattern _ _ _):_,_) = False
      isVariable ([], _) = False
      isVariable _ = error $ "Bad pattern for isVariable"

      isConstr ((L.VarIDPattern _ _):_, _) = False
      isConstr ((L.ConstrPattern _ _ _):_,_) = True
      isConstr ([], _) = False
      isConstr _ = error $ "Bad pattern for isConstr"

      beginsInAllVariables x = all isVariable x
      beginsInAllConstructors x = all isConstr x

      getVarIDFromPattern (L.VarIDPattern _ v) = v

replaceVar :: VarID -> VarID -> L.Expression -> L.Expression
replaceVar a b (L.VariableRef r m)
    | m == a = L.VariableRef r b
replaceVar a b (L.Ap r e1 e2) = L.Ap r (replaceVar a b e1) (replaceVar a b e2)
replaceVar a b (L.LetIn r assertions bindings expression) =
    let assertions' = map replaceAssertion assertions
        bindings' = map (\(L.Binding bindingR bindingPat bindingE) -> L.Binding bindingR bindingPat $ replaceVar a b bindingE) bindings
        expression' = replaceVar a b expression

        replaceAssertion (L.TypeAssertionId m t)
            | m == a = L.TypeAssertionId b t
        replaceAssertion x = x
    in
      L.LetIn r assertions' bindings' expression'
replaceVar a b (L.TypeAssertionE r e t) = L.TypeAssertionE r (replaceVar a b e) t
replaceVar a b (L.Block r es) = L.Block r $ map (replaceVar a b) es
replaceVar a b (L.Lambda r pats e) = L.Lambda r pats $ replaceVar a b e
replaceVar a b (L.Case r e patExprs) = L.Case r (replaceVar a b e) $
                                       map (\(pat, expr) -> (pat, replaceVar a b expr)) patExprs
replaceVar _ _ x = x -- for anything other than variable ref, do nothing