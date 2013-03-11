module Language.Flow.Parse
    (
     parseProgram
    ) where

import Control.Monad hiding (ap)
import Control.Monad.Trans

import qualified Data.Text as T
import Data.List
import Data.Int
import Data.String
import Data.Either
import Data.Maybe

import qualified Language.Flow.AST as L
import Language.Flow.Execution.Types (ModuleName)

import qualified Text.Parsec as P
import qualified Text.Parsec.Token as PT
import qualified Text.Parsec.Expr as PE
import Text.Parsec.Language (haskellDef)
import Text.Parsec.Text
import Text.Parsec hiding (string)

type FlowParser a = ParsecT T.Text () IO a

flowStyle :: PT.GenLanguageDef T.Text () IO
flowStyle = PT.LanguageDef
                { PT.commentStart   = "{-"
                , PT.commentEnd     = "-}"
                , PT.commentLine    = "--"
                , PT.nestedComments = True
                , PT.identStart     = letter
                , PT.identLetter    = alphaNum <|> oneOf "_'"
                , PT.opStart        = PT.opLetter flowStyle
                , PT.opLetter       = oneOf ":!#$%&*+./<=>?@\\^|-~"
                , PT.reservedOpNames= ["=", "::", "->"]
                , PT.reservedNames  = ["let", "in", "Integer", "String", "Double",
                                       "Duration", "DateTime", "TimeSeriesCollection",
                                       "TimeSeries", "import", "qualified", "as"]
                , PT.caseSensitive  = True
                }

-- Define language characteristics
lexer = PT.makeTokenParser flowStyle
lexeme = PT.lexeme lexer
parens = PT.parens lexer
braces = PT.braces lexer
identifier = (lexeme fullyQualified) <|> (PT.identifier lexer) <?> "identifier"
    where
      fullyQualified = do
        components <- fullyQualifiedComponents []
        return $ intercalate "." $ reverse components
      fullyQualifiedComponents xs = do
        component <- fullyQualifiedComponent <|> symbol
        case component of
          Left mod -> fullyQualifiedComponents (mod:xs)
          Right ident -> return $ ident:xs
      fullyQualifiedComponent = do
        firstLetter <- upper
        rest <- many $ PT.identLetter flowStyle
        char '.'
        return $ Left $ firstLetter:rest
      symbol = do
        firstLetter <- lower
        rest <- many $ PT.identLetter flowStyle
        return $ Right $ firstLetter:rest
identifierExpr = do
  start <- getPosition
  name <- identifier
  end <- getPosition
  return $ L.Identifier (L.Region start end) $ fromString name
variableName = do
  firstChar <- lower <|> oneOf "_"
  rest <- many (PT.identLetter flowStyle)
  PT.whiteSpace lexer
  return $ fromString $ firstChar:rest
modName = do
  components <- sepBy1 typeName (char '.')
  let name = intercalate "." components
  return $ fromString name
typeName :: IsString a => FlowParser a
typeName = do
  firstChar <- upper
  rest <- many (PT.identLetter flowStyle)
  PT.whiteSpace lexer
  return $ fromString $ firstChar:rest
reserved = PT.reserved lexer
commaSep = PT.commaSep lexer
semiSep = PT.semiSep lexer
reservedOp = PT.reservedOp lexer
natural = PT.natural lexer

-- Arithmetic expressions
opTable :: PE.OperatorTable T.Text () IO L.Expression
opTable = [[infixOp "**" PE.AssocLeft],
           [infixOp "*" PE.AssocLeft, infixOp "/" PE.AssocLeft],
           [infixOp "+" PE.AssocLeft, infixOp "-" PE.AssocLeft]]

infixOp op assoc = let parser = do
                         start <- getPosition
                         reservedOp op
                         end <- getPosition
                         let r = L.Region start end
                         return $ (\x y -> L.Ap r (L.Ap r (L.Identifier r $ fromString $ "(" ++ op ++ ")") x) y)
                   in PE.Infix parser assoc

expr = PE.buildExpressionParser opTable apOrAtom <?> "expression"

integer, double, string, dateTime, duration :: FlowParser L.Literal
integer = do -- parse integers of the form I<nnnn>
  char 'I'
  invert <- optionMaybe $ char '-'
  digits <- many1 digit
  let integerForm' = (read digits :: Integer)
      integerForm = case invert of
                      Nothing -> integerForm'
                      Just _ -> negate integerForm'
  when (not $ fromIntegral (minBound :: Int64) <= integerForm &&
        integerForm <= fromIntegral (maxBound :: Int64)) $
       fail "Integer value out of bounds"
  return $ L.IntLiteral $ fromIntegral integerForm

double = liftM L.DoubleLiteral $ PT.float lexer

dateTime = fail "dateTime"
duration = fail "duration"

string = liftM L.StringLiteral $
         ( (try singleQuotedMLString) <|> (try doubleQuotedMLString) <|>
           singleQuotedString <|> doubleQuotedString <?> "Expecting string")
    where
      singleQuotedString = singleLineString '\''
      doubleQuotedString = singleLineString '"'

      singleQuotedMLString = multiLineString '\''
      doubleQuotedMLString = multiLineString '"'

      singleLineString delimeter = do -- Parse strings, with potential escape characters
        between (char delimeter) (char delimeter) $
                many ((noneOf $ delimeter:"\\\n\r") <|> (escapeChars delimeter))

      multiLineString delimeter = do
        let delimeter3 = replicate 3 delimeter
        between (P.string delimeter3) (P.string delimeter3) $
                many ((noneOf $ delimeter:"\\\n\r") <|> -- (doubleChar delimeter) <|>
                      (char delimeter) <|> (escapeChars delimeter))

      escapeChars delimeter = choice
                    [try $ escapeChar 'r' '\r',
                     try $ escapeChar 'a' '\a',
                     try $ escapeChar 'b' '\b',
                     try $ escapeChar 'n' '\n',
                     try $ escapeChar 't' '\t',
                     try $ escapeChar delimeter delimeter]

      escapeChar x ret = char '\\' >> char x >> return ret

literal :: FlowParser L.Expression
literal = do
  startPos <- getPosition
  literal <- integer <|> string <|> double <|> dateTime <|> duration <?> "Expecting literal"
  endPos <- getPosition
  return $ L.Literal (L.Region startPos endPos) literal

tsAccessorFunction :: FlowParser L.Expression
tsAccessorFunction = do
  start <- getPosition
  args <- many1 element
  end <- getPosition
  let r = L.Region start end
      accessorList = foldr (\x y -> L.Ap r (L.Ap r (L.Identifier r $ fromString "Cons") x) y) (L.Identifier r $ fromString "Null") args
  return $ L.Ap r (L.Identifier r $ fromString "mkTSAccessor") accessorList
  where
    element = do
      start <- getPosition
      char ':'
      firstChar <- PT.identStart flowStyle
      rest <- many $ PT.identLetter flowStyle
      end <- getPosition
      return $ L.Literal (L.Region start end) $ L.StringLiteral $ firstChar:rest

accessorFunction :: FlowParser L.Expression
accessorFunction = do
  start <- getPosition
  char '.'
  firstChar <- PT.identStart flowStyle
  rest <- many $ PT.identLetter flowStyle
  end <- getPosition
  let r = L.Region start end
  return $ L.Ap r (L.Identifier r $ fromString "mkAccessor") (L.Literal r $ L.StringLiteral $ firstChar:rest)

timeSeriesCollection :: FlowParser L.Expression
timeSeriesCollection = do
  start <- getPosition
  char '#'
  firstLetter <- PT.identStart flowStyle
  collectionNameXs <- many $ PT.identLetter flowStyle
  end <- getPosition
  let collectionName = firstLetter:collectionNameXs
      r = L.Region start end
      ret = L.Ap r (L.Identifier r $ fromString "timeSeries") (L.Literal r $ L.StringLiteral collectionName)
  (try $ do
     lookAhead $ char ':' -- check if there's a colon next
     func <- tsAccessorFunction
     return $ L.Ap (L.exprRegion func) func ret) <|>
   (try $ do
      lookAhead $ char '.' -- check if there's a dot next
      func <- accessorFunction
      return $ L.Ap (L.exprRegion func) func ret) <|> ((PT.whiteSpace lexer) >> return ret)

lambda :: FlowParser L.Expression
lambda = do
  start <- getPosition
  reservedOp "\\"
  args <- many1 pattern
  reservedOp "->"
  e <- expr
  end <- getPosition
  return $ L.Lambda (L.Region start end) args e

aexp :: FlowParser L.Expression -- expression parser for only non application nodes. Helps prevent left-recursion
aexp = (do
         start <- getPosition
         baseExpr <- (parens expr) <|>  timeSeriesCollection <|> (lexeme tsAccessorFunction) <|>
                     (lexeme accessorFunction) <|> lambda  <|> letIn <|> (try $ lexeme literal) <|>
                     identifierExpr <?> "expression"
         choice [try $ do
                   reservedOp "::"
                   t <- flowType
                   end <- getPosition
                   return $ L.TypeAssertionE (L.Region start end) baseExpr t
                 ,
                 return baseExpr]
       ) <?> "expression"

typeAssertionExpr :: FlowParser L.Expression
typeAssertionExpr = do
  start <- getPosition
  l <- aexp
  reservedOp "::"
  t <- flowType
  end <- getPosition
  return $ L.TypeAssertionE (L.Region start end) l t

letIn :: FlowParser L.Expression
letIn = do
  start <- getPosition
  reserved "let"
  (bindings, typeAssertions) <- letDefs
  reserved "in"
  body <- letBody
  end <- getPosition
  return $ L.LetIn (L.Region start end) typeAssertions bindings body

pattern :: FlowParser L.Pattern
pattern = parens pattern <|>
          literalPat <|>
          constrPat <|>
          varName <?> "pattern"

-- constrOrVarPattern = (try constrOrVarPattern') <|> parens constrOrVarPattern <?> "constructor or variable pattern"
-- constrOrVarPattern' = parens constrPat <|> varName

literalPat = do
  L.Literal r l <- literal
  case l of
    L.IntLiteral i -> return $ L.ConstrPattern r (fromString $ "I" ++ show i) []
    L.StringLiteral s -> return $ L.ConstrPattern r (fromString $ show s) []
    L.DoubleLiteral d -> return $ L.ConstrPattern r (fromString $ show d) []

varName = do
  start <- getPosition
  name <- variableName
  end <- getPosition
  return $ L.VariablePattern (L.Region start end) name

constrPat = do
  start <- getPosition
  constrName <- typeName
  args <- many1 (parens constrPat <|> literalPat <|> varName)
  end <- getPosition
  return $ L.ConstrPattern (L.Region start end) constrName args

flowType :: FlowParser L.Type
flowType = (try fnType) <|>
           nonFnType <?> "type"
    where
      primType = integerType <|> stringType <|> doubleType <|> durationType <|>
                 dateTimeType <|> typeVar <?> "primitive type"

      nonFnType = parens flowType <|>
                  primType <|>
                  timeSeriesCollectionType <|>
                  timeSeriesType <?> "non-function type"

      typeVar = liftM L.TypeVariable variableName

      fnType = do
        a <- nonFnType
        reservedOp "->"
        b <- flowType
        return $ L.FnType a b

      timeSeriesCollectionType = do
        reserved "TimeSeriesCollection"
        subType <- primType
        return $ L.TimeSeriesCollectionType subType

      timeSeriesType = do
        reserved "TimeSeries"
        subType <- primType
        try (do
              frequency <- natural
              return $ L.TimeSeriesTypeAbs subType frequency) <|>
         do
           varName <- variableName
           return $ L.TimeSeriesTypeVar subType varName

      integerType = typeName "Integer" L.IntegerType
      stringType = typeName "String" L.StringType
      doubleType = typeName "Double" L.DoubleType
      durationType = typeName "Duration" L.DurationType
      dateTimeType = typeName "DateTime" L.DateTimeType

      typeName name r = do
        reserved name
        return r

letDefs :: FlowParser ([L.Binding], [L.TypeAssertion])
letDefs = braces $ do
            bindingsOrTypeAssertions <- semiSep bindingOrTypeAssertion
            return $ partitionEithers bindingsOrTypeAssertions
    where
      bindingOrTypeAssertion :: FlowParser (Either L.Binding L.TypeAssertion)
      bindingOrTypeAssertion = (try functionBinding) <|> (try binding) <|> letTypeAssertion <?> "binding or type assertion"

      functionBinding = do
        start <- getPosition
        funcName <- variableName
        endVarName <- getPosition
        args <- many pattern
        reservedOp "="
        value <- expr
        end <- getPosition

        let nameRegion = L.Region start endVarName
            region = L.Region start end
            bindingPat = L.VariablePattern nameRegion funcName
        case args of
          [] -> return $ Left $ L.Binding region bindingPat value -- variable
          xs -> return $ Left $ L.Binding region bindingPat $ L.Lambda region args value

      binding = do
        start <- getPosition
        var <- pattern
        reservedOp "="
        value <- expr
        end <- getPosition
        return $ Left $ L.Binding (L.Region start end) var value

      letTypeAssertion = do
        var <- variableName
        reservedOp "::"
        t <- flowType
        return $ Right $ L.TypeAssertion var t

letBody :: FlowParser L.Expression
letBody = braces letBlock <|> expr
    where
      letBlock = do
        start <- getPosition
        exprs <- semiSep expr
        end <- getPosition
        return $ L.Block (L.Region start end) exprs

apOrAtom :: FlowParser L.Expression
apOrAtom = do
  start <- getPosition
  f <- aexp
  args <- many aexp
  end <- getPosition
  return $ applyArguments (L.Region start end) f args
 where
   applyArguments r f [] = f
   applyArguments r f (arg:args) = applyArguments r (L.Ap r f arg) args

importDecl :: FlowParser (ModuleName, ModuleName, Bool)
importDecl = do
  reserved "import"
  isGlobal <- (try $ reserved "qualified" >> return False ) <|> (return True)
  name <- modName
  pseudonym <- (try $ reserved "as" >> modName) <|> return name
  PT.semi lexer
  return (name, pseudonym, isGlobal)

program :: FlowParser L.Program
program = do
  imports <- many importDecl
  let globalImports = mapMaybe (\(_, pseudonym, isGlobal) -> if isGlobal then Just pseudonym else Nothing) imports
      imports' = map (\(name, pseudonym, _) -> (name, pseudonym)) imports

  universalProgramImports <- liftIO $ L.getUniversalImports
  let universalGlobalImports = map fst universalProgramImports

  body <- expr
  eof
  return $ L.Program { L.flowProgramImports = universalProgramImports ++ imports',
                       L.flowGlobalImports = universalGlobalImports ++ globalImports,
                       L.flowProgramBody = body }

parseProgram :: FilePath -> T.Text -> IO L.Program
parseProgram filePath progTxt = do
  res <- runParserT program () filePath progTxt
  case res of
    Left e -> fail $ show e
    Right x -> return x