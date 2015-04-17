#pragma once

#include "env.h"

class Topic {
public:
    Topic(Env* env);
    ~Topic();

private:
    Env *_env;
};