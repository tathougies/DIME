module Database.DIME.Flow
    (module Database.DIME.Flow.Types,
     initFlowEngine)
        where
    import Language.Flow.Module
    import Language.Flow.Compile
    import Language.Flow.AST
    import Language.Flow.Builtin
    import Language.Flow.Execution.Types

    import qualified Data.Map as M
    import Data.IORef
    import Data.String

    import Database.DIME.Flow.Builtin
    import Database.DIME.Flow.Types

    -- Common definitions for the Flow engine in the DIME database

    initFlowEngine :: IO ()
    initFlowEngine = do
      initFlowLanguageBuiltins
      registerBuiltinModule dimeBuiltin
      addUniversalImport (fromString "DIME") -- automatically import the DIME module