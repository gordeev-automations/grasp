# TESTS_TO_RUN = `pwd`/test/*.test.grasp
TESTS_TO_RUN = `pwd`/test/json_matching.test.grasp #`pwd`/test/basic.test.grasp `pwd`/test/joins.test.grasp `pwd`/test/only_facts.test.grasp

ensure_transpiler_ready:
	PYTHONPATH=src python ./src/grasp/scripts/ensure_transpiler_ready.py

test: ensure_transpiler_ready
	PYTHONPATH=src python ./src/grasp/scripts/ensure_tests_transpiled.py $(TESTS_TO_RUN)
	PYTHONPATH=src python ./src/grasp/scripts/compile_and_run_tests.py $(TESTS_TO_RUN)

.PHONY: ensure_transpiler_ready test