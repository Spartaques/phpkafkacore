<?php

// autoload_static.php @generated by Composer

namespace Composer\Autoload;

class ComposerStaticInitf3ab48c25b087f2a0d66def1901ae51a
{
    public static $prefixLengthsPsr4 = array (
        'A' => 
        array (
            'App\\' => 4,
        ),
    );

    public static $prefixDirsPsr4 = array (
        'App\\' => 
        array (
            0 => __DIR__ . '/../..' . '/src',
        ),
    );

    public static function getInitializer(ClassLoader $loader)
    {
        return \Closure::bind(function () use ($loader) {
            $loader->prefixLengthsPsr4 = ComposerStaticInitf3ab48c25b087f2a0d66def1901ae51a::$prefixLengthsPsr4;
            $loader->prefixDirsPsr4 = ComposerStaticInitf3ab48c25b087f2a0d66def1901ae51a::$prefixDirsPsr4;

        }, null, ClassLoader::class);
    }
}
