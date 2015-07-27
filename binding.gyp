{
  "targets": [
    {
      "target_name": "lmdb-queue",
	  "include_dirs" : [
        "<!(node -e \"require('nan')\")",
        "deps"
      ],
      "dependencies": [
        "<(module_root_dir)/deps/lmdb.gyp:lmdb"
      ],
      "sources": [
        "src/module.cc",
        "src/env.h",
        "src/env.cc",
        "src/topic.h",
        "src/topic.cc",
        "src/consumer.h",
        "src/consumer.cc",
        "src/producer.h",
        "src/producer.cc",
        "src/wrapper.h"
      ],
      "conditions": [
        [
          "OS == 'win'", {
            'msvs_settings': {
              'VCCLCompilerTool': {
                'RuntimeTypeInfo': 'false',
                'EnableFunctionLevelLinking': 'true',
                'DisableSpecificWarnings': [ '4267' ],
                'AdditionalOptions': ['/EHsc']
              }
            }
          }
        ],
        [
          "OS=='linux'", {
            "cflags_cc": [ "-std=c++11" ]
          }
        ],
        [
          'OS == "mac"', {
            'xcode_settings': {
              'MACOSX_DEPLOYMENT_TARGET': '10.7',
              'OTHER_CPLUSPLUSFLAGS': [
                  '-std=c++11' ,
                  '-stdlib=libc++'
              ]
            }
          }
        ]
      ]
    }
  ]
}