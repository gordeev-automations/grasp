ensure_transpiler_ready:
	PYTHONPATH=. python ./scripts/ensure_transpiler_ready.py

test: ensure_transpiler_ready
	PYTHONPATH=. python ./scripts/ensure_tests_transpiled.py `pwd`/test/json_matching.test.grasp #`pwd`/test/*.test.grasp
	PYTHONPATH=. python ./scripts/compile_and_run_tests.py `pwd`/test/json_matching.test.grasp #`pwd`/test/*.test.grasp

.PHONY: ensure_transpiler_ready test