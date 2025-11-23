ensure_transpiler_ready:
	PYTHONPATH=.. python ./scripts/ensure_transpiler_ready.py

test: ensure_transpiler_ready
	PYTHONPATH=.. python ./scripts/ensure_tests_transpiled.py `pwd`/test/*.test.grasp

.PHONY: ensure_transpiler_ready test