{-# LANGUAGE GeneralizedNewtypeDeriving, BangPatterns, ExistentialQuantification, RankNTypes, DeriveDataTypeable, RecordWildCards #-}
module Language.Flow.Execution.Types where

import qualified Data.IntSet as IntSet
import Data.Map as M
import Data.Array.IO
import Data.Array as A
import Data.Word
import Data.Either
import Data.Int
import Data.Text
import Data.String
import Data.Binary
import Data.Dynamic
import Data.IORef

import Control.Monad
import Control.Monad.Trans
import Control.Exception

import System.IO.Unsafe

import Text.JSON

import Unsafe.Coerce

newtype GMachineAddress = GMachineAddress Int
    deriving (Ord, Eq, Enum, Show, Read, Num, Ix, Integral, Real, Binary)
newtype StackOffset = StackOffset Int
    deriving (Ord, Eq, Enum, Show, Read, Num, Ix, Integral, Real, Binary)
newtype VarID = VarID Int
    deriving (Ord, Eq, Show, Read, Num, Integral, Real, Enum)
newtype SCName = SCName Int
    deriving (Ord, Eq, Show, Read, Num, Integral, Real, Enum)
newtype VariableName = VariableName Text
    deriving (Ord, Eq, Show, Read, JSON, IsString, Binary)
newtype TypeName = TypeName Text
    deriving (Ord, Eq, Show, Read, JSON, IsString, Binary)
newtype ModuleName = ModuleName Text
    deriving (Ord, Eq, Show, Read, JSON, IsString, Binary)

type Label = Int
type GMachineStack = [GMachineAddress]
newtype GCodeBuiltin = GCodeBuiltin ([GMachineAddress] -> GMachine (Maybe GenericGData))
type GCodeSequence = [GCode]
type GTypeName = Text
type GFieldName = Text
type GConstr = Text -- type of constructor names
type GConstrArgs = Array Int GMachineAddress

data GMachineError = GMachineError GMachineState String
                     deriving Typeable

data GMachine a =
    GMachine { runGMachine :: GMachineState -> IO (Either GMachineError (GMachineState, a)) }

data GCodeProgram =
    GCodeProgram {
      initCode :: GCodeSequence,
      initialData :: [(GMachineAddress, GenericGData)]
    }
    deriving (Show)

data GMachineState = GMachineState {
      gmachineStack :: GMachineStack,
      gmachineGraph :: IOArray GMachineAddress GenericGData,
      gmachineCode :: GCodeSequence,
      gmachineDump :: [(GMachineStack, GCodeSequence)],
      gmachineInitData :: [(GMachineAddress, GenericGData)],
      gmachineFreeCells :: IntSet.IntSet,
      gmachineIndent :: Int,
      gmachineDebug :: Bool,
      gmachineUserState :: Dynamic
    }

data GMachineFrozenState = GMachineFrozenState {
      gmachineFrozenStack :: GMachineStack,
      gmachineFrozenGraph :: Array GMachineAddress GenericGData,
      gmachineFrozenCode :: GCodeSequence,
      gmachineFrozenDump :: [(GMachineStack, GCodeSequence)],
      gmachineFrozenInitData :: [GMachineAddress],
      gmachineFrozenFreeCells :: IntSet.IntSet,
      gmachineFrozenDebug :: Bool
    }
 deriving (Show)

instance Read GMachineFrozenState where -- necessary so that MapOperation is Read'able
    readsPrec = error "Can't read GMachineFrozenState"

instance Binary GMachineFrozenState where
    put GMachineFrozenState {..} = do
      put gmachineFrozenStack
      put gmachineFrozenGraph
      put gmachineFrozenCode
      put gmachineFrozenDump
      put gmachineFrozenInitData
      put gmachineFrozenFreeCells
      put gmachineFrozenDebug

    get = return GMachineFrozenState `ap` get `ap` get `ap` get `ap`
          get `ap` get `ap` get `ap` get

data Module = Module {
            flowModuleName :: ModuleName,
            flowModuleMembers :: M.Map VariableName GenericGData
    }
 deriving Show

{-# NOINLINE builtinModules #-}
builtinModules :: IORef (M.Map ModuleName Module)
builtinModules = unsafePerformIO $ do
                   newIORef $ M.fromList $ Prelude.map (\m -> (flowModuleName m, m)) []

getBuiltinModules :: IO (M.Map ModuleName Module)
getBuiltinModules = readIORef builtinModules

registerBuiltinModule :: Module -> IO ()
registerBuiltinModule mod =
    modifyIORef builtinModules $
                M.insert (flowModuleName mod) mod

-- | G-Machine instructions as found in Simon Peyton-Jones's book w/ some modifications
data GCode =
    -- State transitions (control)
    Eval |
    Unwind |
    Return |
    Jump {-# UNPACK #-} !Label |
    JFalse {-# UNPACK #-} !Label |

    -- Data examination
    Examine Text {-# UNPACK #-} !Int {-# UNPACK #-} !Label |

    -- Data manipulation
    Push {-# UNPACK #-} !StackOffset |
    PushInt {-# UNPACK #-} !Int64 |
    PushString !Text |
    PushDouble {-# UNPACK #-} !Double |
    PushLocation {-# UNPACK #-} !GMachineAddress |
    Pop {-# UNPACK #-} !StackOffset |
    Slide {-# UNPACK #-} !StackOffset |
    Update {-# UNPACK #-} !StackOffset |
    Alloc {-# UNPACK #-} !Int |
    MkAp |

    -- Internal codes
    CallBuiltin |
    ProgramDone
    deriving (Show, Eq)

instance Binary GCode where
    put Eval = put (0 :: Int8)
    put Unwind = put (1 :: Int8)
    put Return = put (2 :: Int8)
    put (Jump l) = do
                 put (3 :: Int8)
                 put l
    put (JFalse l) = do
                 put (4 :: Int8)
                 put l
    put (Examine constr arity offset) = do
                 put (5 :: Int8)
                 put constr
                 put arity
                 put offset
    put (Push stackOfs) = do
                 put (6 :: Int8)
                 put stackOfs
    put (PushInt i) = do
                 put (7 :: Int8)
                 put i
    put (PushString s) = do
                 put (8 :: Int8)
                 put s
    put (PushDouble d) = do
                 put (9 :: Int8)
                 put d
    put (PushLocation l) = do
                 put (10 :: Int8)
                 put l
    put (Pop stackOfs) = do
                 put (11 :: Int8)
                 put stackOfs
    put (Slide stackOfs) = do
                 put (12 :: Int8)
                 put stackOfs
    put (Update stackOfs) = do
                 put (13 :: Int8)
                 put stackOfs
    put (Alloc count) = do
                 put (14 :: Int8)
                 put count
    put MkAp = put (15 :: Int8)
    put CallBuiltin = put (16 :: Int8)
    put ProgramDone = put (17 :: Int8)

    get = do
      tag <- (get :: Get Int8)
      case tag of
        0 -> return Eval
        1 -> return Unwind
        2 -> return Return
        3 -> doJump
        4 -> doJFalse
        5 -> doExamine
        6 -> doPush
        7 -> doPushInt
        8 -> doPushString
        9 -> doPushDouble
        10 -> doPushLocation
        11 -> doPop
        12 -> doSlide
        13 -> doUpdate
        14 -> doAlloc
        15 -> return MkAp
        16 -> return CallBuiltin
        17 -> return ProgramDone

     where
       doJump = liftM Jump get
       doJFalse = liftM JFalse get
       doExamine = liftM3 Examine get get get
       doPush = liftM Push get
       doPushInt = liftM PushInt get
       doPushString = liftM PushString get
       doPushDouble = liftM PushDouble get
       doPushLocation = liftM PushLocation get
       doPop = liftM Pop get
       doSlide = liftM Slide get
       doUpdate = liftM Update get
       doAlloc = liftM Alloc get

-- Data

data GenericGData = forall a. GData a => G a |
                    Hole

mkGeneric :: GData a => a -> GenericGData
mkGeneric = G

withGenericData :: (forall a. GData a => a -> b) -> GenericGData -> b
withGenericData f Hole = error "withGenericData: Hole"
withGenericData f (G x) = f x

usingGenericData = flip withGenericData

-- | A generic class for all data types that can be put into the G-Machine graph
-- it contains functions that are necessary for the type checker, the constructor
-- matching system, and the garbage collector
class (Show a) => GData a where
    -- | Returns the type name of the this data. This function may be strict in its first argument
    typeName :: a -> GTypeName

    -- | Returns the name of the constructor used to constract this data type
    constr :: a -> GConstr

    -- | The arguments used to construct this data point. This should also be all things
    -- referenced by this data point.
    constrArgs :: a -> GConstrArgs

    -- | For performance reasons. Do not overwrite in your own code!
    isBuiltin :: a -> Bool
    isBuiltin _ = False

    -- | Access the field specified (by default spits out an error message)
    getField :: GFieldName -> a -> GMachine GenericGData
    getField f x = throwError $ "Cannot get field " ++ show f ++ " from object of type " ++ unpack (typeName x)

    -- | Determine if this type supports a generic operation
    supportsGeneric :: a -> String -> Bool
    supportsGeneric _ _ = False

    -- | Run a generic operation on this type
    runGeneric :: a -> String -> [GenericGData] -> GMachine (Maybe GenericGData)
    runGeneric a gen = error $ "Generic " ++ gen ++ " not supported by " ++ (unpack $ typeName a)

{-# NOINLINE gDataConstrFuncsVar #-}
gDataConstrFuncsVar :: IORef (Map GTypeName (GConstr -> [GMachineAddress] -> GenericGData))
gDataConstrFuncsVar = unsafePerformIO $ newIORef M.empty

gDataConstrFuncs :: Map GTypeName (GConstr -> [GMachineAddress] -> GenericGData)
gDataConstrFuncs = unsafePerformIO $ readIORef gDataConstrFuncsVar

registerGData :: GData a => a -> (GConstr -> [GMachineAddress] -> GenericGData) -> IO ()
registerGData gData gConstrFunc =
  modifyIORef gDataConstrFuncsVar $ M.insert (typeName gData) gConstrFunc

instance Show GenericGData where
    show Hole = "Hole"
    show x = withGenericData show x

instance Binary Text where
    put = put . unpack
    get = liftM pack get

instance Binary GenericGData where
    put Hole = put (-1 :: Int8)
    put x
     | isInteger x = do
           put (1 :: Int8)
           put (asInteger x)
     | isString x = do
           put (2 :: Int8)
           put (asString x)
     | isDouble x = do
           put (3 :: Int8)
           put (asDouble x)
     | withGenericData isBuiltin x =
           case withGenericData asBuiltin x of
             Ap a b -> do
               put (4 :: Int8)
               put a
               put b
             Fun arity code -> do
               put (5 :: Int8)
               put arity
               put code
             BuiltinFun _ modName symName _ -> do
               put (6 :: Int8)
               put modName
               put symName
     | otherwise = do
           put (0 :: Int8)
           put (withGenericData typeName x)
           put (withGenericData constr x)
           put (A.elems $ withGenericData constrArgs x)

    get = do
      tag <- (get :: Get Int8)
      case tag of
        -1 -> return Hole
        1 {- IntConstant -} -> liftM (mkGeneric . IntConstant) get
        2 {- StringConstant -} -> liftM (mkGeneric . StringConstant) get
        3 {- DoubleConstant -} -> liftM (mkGeneric . DoubleConstant) get
        4 {- BuiltinData Ap -} -> do
                               a <- get
                               b <- get
                               return $ mkGeneric $ Ap a b
        5 {- BuiltinData Fun -} -> do
                               arity <- get
                               code <- get
                               return $ mkGeneric $ Fun arity code
        6 {- BuiltinData BuiltinFun -} -> do
                               modName <- get
                               symName <- get
                               let allBuiltinModules = unsafePerformIO $ readIORef builtinModules
                               case M.lookup modName allBuiltinModules of
                                 Nothing -> error $ "Could not find builtin module " ++ show modName
                                 Just Module { flowModuleMembers = mod } ->
                                     case M.lookup symName mod of
                                       Nothing -> error $ "Could not find " ++ show symName ++ " in " ++ show modName
                                       Just sym -> return sym
        0 -> do
            typeName <- (get :: Get GTypeName)
            case M.lookup typeName gDataConstrFuncs of
              Nothing -> fail $ "Could not find constructor function for type " ++ show typeName
              Just constrFunc -> do
                               constrName <- (get :: Get GConstr)
                               constrArgs <- (get :: Get [GMachineAddress])
                               return $ constrFunc constrName constrArgs

-- Data types

checkCoerce :: GTypeName -> GenericGData -> b
checkCoerce name x = if checkType name x then withGenericData unsafeCoerce x else error $ "checkCoerce " ++ show x

checkType :: GTypeName -> GenericGData -> Bool
checkType name = withGenericData (\gData -> typeName gData == name)

newtype IntConstant = IntConstant Int64
    deriving (Ord, Eq, Enum, Num, Real, Integral, Show, Read)

isInteger :: GenericGData -> Bool
isInteger = checkType (typeName (undefined :: IntConstant))

asInteger :: GenericGData -> Int64
asInteger = checkCoerce (typeName (undefined :: IntConstant))

instance GData IntConstant where
    typeName _ = fromString "Integer"
    constr x = fromString $ "I" ++ show (fromIntegral x :: Integer)
    constrArgs _ = array (0,-1) []

newtype StringConstant = StringConstant Text
    deriving (Ord, Eq, IsString, Show, Read)

isString :: GenericGData -> Bool
isString = checkType (typeName (undefined :: StringConstant))

asString :: GenericGData -> Text
asString = checkCoerce (typeName (undefined :: StringConstant))

instance GData StringConstant where
    typeName _ = fromString "String"
    constr (StringConstant x) = fromString $ show x
    constrArgs _ = array (0,-1) []

newtype DoubleConstant = DoubleConstant Double
    deriving (Show, Read, Ord, Eq, Real, Num, Fractional)

isDouble :: GenericGData -> Bool
isDouble = checkType (typeName (undefined :: DoubleConstant))

asDouble :: GenericGData -> Double
asDouble = checkCoerce (typeName (undefined :: DoubleConstant))

instance GData DoubleConstant where
    typeName _ = fromString "Double"
    constr x = fromString $ show x
    constrArgs _ = array (0,-1) []

data BuiltinData = Ap {-# UNPACK #-} !GMachineAddress {-# UNPACK #-} !GMachineAddress |
                   Fun {-# UNPACK #-} !Int GCodeSequence |
                   BuiltinFun {-# UNPACK #-} !Int !ModuleName !VariableName GCodeBuiltin

asBuiltin :: GData a => a -> BuiltinData
asBuiltin dat = if isBuiltin dat then unsafeCoerce dat else
                    error $ "Cannot coerce builtin data for " ++ show dat

instance GData BuiltinData where
    typeName _ = fromString "BuiltinData"
    constr _ = error "Attempt to examine constructor of builtin data"
    constrArgs (Ap a1 a2) = listArray (0, 1) [a1, a2]
    constrArgs _ = array (0,-1) []
    isBuiltin _ = True

instance Show BuiltinData where
    show (Ap a b) = "Ap (" ++ show a ++ ") (" ++ show b ++ ")"
    show (Fun i s) = "Fun " ++ show i ++ " " ++ show s
    show (BuiltinFun i modName symbolName _) = "BuiltinFun " ++ show i ++ " (" ++ show symbolName ++ " from " ++ show modName ++ ")"

instance Show GMachineError where
    show (GMachineError _ e) = e

instance Exception GMachineError

todo :: MonadIO m => String -> m ()
todo = liftIO . putStrLn . ("TODO: " ++)

-- | Throw an error from within the GMachine monad
throwError :: String -> GMachine a
throwError errorMsg = GMachine (\st ->
                                  return $ Left $ GMachineError st errorMsg)

-- | Rethrow an error from within the GMachine monad
throwError' :: GMachineError -> GMachine a
throwError' e = GMachine (\st -> return $ Left e)