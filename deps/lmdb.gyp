{
  "targets": [
    {
      "target_name": "lmdb",
      "type": "static_library",
      "standalone_static_library": 1,
      "defines": [
      ],
      "sources": [
        "lmdb/midl.c",
        "lmdb/mdb.c",
      ],
      "conditions": [
        [
          "OS == 'win'", {
            "msvs_settings": {
              "VCCLCompilerTool": {
                "EnableFunctionLevelLinking": "true",
                "DisableSpecificWarnings": [ "4024", "4047", "4146", "4244", "4267", "4996" ]
              }
            }
          }
        ]
      ]
    }
  ]
}