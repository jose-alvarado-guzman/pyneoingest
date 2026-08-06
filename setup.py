import setuptools

setuptools.setup(
    packages=setuptools.find_packages(exclude=["test", "test.*"])
)
