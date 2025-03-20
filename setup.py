from setuptools import setup, find_packages

setup(
    name="kalshi_dashboard",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'streamlit',
        'requests',
        'python-dotenv',
        'pandas',
        'cryptography',
        'websockets',
        'asgiref'
    ]
)
