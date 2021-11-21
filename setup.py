import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as reqs:
    install_requires = reqs.readlines()

setuptools.setup(
    name="nbt",
    version="0.0.1",
    author="Steven Brien",
    author_email="spbrien@gmail.com",
    description="Utilities packages",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/spbrien/nobedtimes-utils",
    project_urls={
        "Bug Tracker": "https://github.com/spbrien/nobedtimes-utils/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: UNLICENSED",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=install_requires
)