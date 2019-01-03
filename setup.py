from setuptools import setup


def build_native(spec):
    # build an example rust library
    build = spec.add_external_build(
        cmd=['cargo', 'build', '--release'],
        path='./rust'
    )

    spec.add_cffi_module(
        module_path='slonik._native',
        dylib=lambda: build.find_dylib('postgres_ffi', in_path='target/release'),
        header_filename=lambda: build.find_header('postgres_ffi.h', in_path='target'),
        rtld_flags=['NOW', 'NODELETE']
    )


setup(
    name='slonik',
    version='0.0.1',
    packages=['slonik'],
    zip_safe=False,
    platforms='any',
    setup_requires=['milksnake'],
    install_requires=['milksnake'],
    extras_require={
        'tests': ['pytest'],
    },
    milksnake_tasks=[
        build_native
    ]
)
