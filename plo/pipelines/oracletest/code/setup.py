from setuptools import setup, find_packages
setup(
    name = 'oracletest',
    version = '1.0',
    packages = find_packages(include = ('oracletest*', )) + ['prophecy_config_instances.oracletest'],
    package_dir = {'prophecy_config_instances.oracletest' : 'configs/resources/oracletest'},
    package_data = {'prophecy_config_instances.oracletest' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.24'],
    entry_points = {
'console_scripts' : [
'main = oracletest.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
