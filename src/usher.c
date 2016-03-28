/*
 *  usher.c
 *  lua-usher
 *
 *  Created by Masatoshi Teruya on 14/08/07.
 *
 *  Copyright 2014 Masatoshi Teruya. All rights reserved.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

#include <errno.h>
#include <lauxlib.h>
#include <usher.h>

#define MODULE_MT   "lusher"

// memory alloc/dealloc
#define palloc(t)       (t*)malloc( sizeof(t) )
#define pdealloc(p)     free((void*)p)

// helper macros for lua_State
#define lstate_fn2tbl(L,k,v) do{ \
    lua_pushstring(L,k); \
    lua_pushcfunction(L,v); \
    lua_rawset(L,-3); \
}while(0)


#define lstate_int2tbl(L,k,v) do{ \
    lua_pushstring(L,k); \
    lua_pushinteger(L,v); \
    lua_rawset(L,-3); \
}while(0)


#define lstate_ref(L,idx) \
    (lua_pushvalue(L,idx),luaL_ref( L, LUA_REGISTRYINDEX ))

#define lstate_pushref(L,ref) \
    lua_rawgeti( L, LUA_REGISTRYINDEX, ref )

#define lstate_unref(L,ref) \
    luaL_unref( L, LUA_REGISTRYINDEX, (ref) )

#define lstate_isref(ref)   ((ref) > 0)


typedef struct {
    usher_t *usher;
} lusher_t;

typedef struct {
    lua_State *L;
    int type;
    int64_t data;
} lusher_udata_t;


static int set_lua( lua_State *L )
{
    lusher_t *u = luaL_checkudata( L, 1, MODULE_MT );
    size_t len = 0;
    const char *key = luaL_checklstring( L, 2, &len );
    lusher_udata_t *udata = NULL;
    
    // remove segment of passed path if value is nil or none
    if( lua_isnoneornil( L, 3 ) )
    {
        usher_error_t err = usher_remove( u->usher, key );
        
        if( err == USHER_OK ){
            return 0;
        }
        lua_pushstring( L, usher_strerror( err ) );
        lua_pushinteger( L, err );
    }
    else if( ( udata = palloc( lusher_udata_t ) ) )
    {
        usher_error_t err;
        
        // check type
        udata->type = lua_type( L, 3 );
        switch( udata->type )
        {
            // should copy data
            case LUA_TNUMBER:
                udata->data = (int64_t)lua_tonumber( L, 3 );
            break;
            case LUA_TBOOLEAN:
                udata->data = (int)lua_toboolean( L, 3 );
            break;
            
            // should retain reference
            case LUA_TSTRING:
            case LUA_TTABLE:
            case LUA_TFUNCTION:
            case LUA_TUSERDATA:
            case LUA_TTHREAD:
            case LUA_TLIGHTUSERDATA:
                udata->data = (int)lstate_ref( L, 3 );
            break;
        }
        
        err = usher_replace( u->usher, key, (void*)udata );
        if( err == USHER_OK ){
            udata->L = L;
            return 0;
        }
        
        // got error
        lua_pushstring( L, usher_strerror( err ) );
        lua_pushinteger( L, err );
        pdealloc( udata );
    }
    // no-mem
    else {
        lua_pushstring( L, strerror( errno ) );
        lua_pushinteger( L, USHER_ENOMEM );
    }
    
    return 2;
}


static inline void push_udata( lua_State *L, lusher_udata_t *udata )
{
    // check type
    switch( udata->type )
    {
        // copied data
        case LUA_TNUMBER:
            lua_pushnumber( L, (lua_Number)udata->data );
        break;
        case LUA_TBOOLEAN:
            lua_pushboolean( L, (int)udata->data );
        break;
        
        // referenced data
        case LUA_TSTRING:
        case LUA_TTABLE:
        case LUA_TFUNCTION:
        case LUA_TUSERDATA:
        case LUA_TTHREAD:
        case LUA_TLIGHTUSERDATA:
            lstate_pushref( L, (int)udata->data );
        break;
        
        // ignore nil or none value
        // case LUA_TNIL:
        default:
            lua_pushnil( L );
    }
}


static int get_lua( lua_State *L )
{
    lusher_t *u = luaL_checkudata( L, 1, MODULE_MT );
    size_t len = 0;
    const char *key = luaL_checklstring( L, 2, &len );
    usher_state_t state;
    
    if( usher_get( u->usher, key, &state ) == USHER_MATCH &&
        state.seg->type & USHER_SEG_EOS ){
        push_udata( L, (lusher_udata_t*)state.seg->udata );
        return 1;
    }
    
    return 0;
}


static int exec_lua( lua_State *L )
{
    lusher_t *u = luaL_checkudata( L, 1, MODULE_MT );
    size_t len = 0;
    const char *key = luaL_checklstring( L, 2, &len );
    usher_glob_t glob;
    usher_error_t err = usher_exec( u->usher, key, &glob );

    // got mem-error
    if( err == USHER_ENOMEM ){
        lua_pushnil( L );
        lua_pushnil( L );
        lua_pushstring( L, usher_strerror( err ) );
        lua_pushinteger( L, err );
        return 4;
    }
    // found eos
    else if( err == USHER_OK && glob.seg->type & USHER_SEG_EOS ){
        push_udata( L, (lusher_udata_t*)glob.seg->udata );
        // eliminate a glob.eos
        glob.eos = NULL;
    }
    else {
        lua_pushnil( L );
    }

    // push glob items
    if( glob.nitems )
    {
        size_t i = 0;
        
        lua_createtable( L, 0, glob.nitems );
        for(; i < glob.nitems; i++ ){
            lua_pushstring( L, (char*)glob.items[i].name );
            lua_pushlstring( L, (char*)glob.items[i].head,
                             glob.items[i].tail - glob.items[i].head );
            lua_rawset( L, -3 );
        }

        if( glob.eos ){
            goto PUSH_GLOB_EOS;
        }
    }
    // push glob eos
    else if( glob.eos ){
        lua_createtable( L, 1, 0 );

PUSH_GLOB_EOS:
        push_udata( L, (lusher_udata_t*)glob.eos->udata );
        lua_rawseti( L, -2, 1 );
    }
    else {
        lua_pushnil( L );
    }

    usher_glob_dealloc( &glob );

    return 2;
}


static int dump_lua( lua_State *L )
{
    lusher_t *u = luaL_checkudata( L, 1, MODULE_MT );
    
    usher_dump( u->usher );
    
    return 0;
}


static int tostring_lua( lua_State *L )
{
    lua_pushfstring( L, MODULE_MT ": %p", lua_touserdata( L, 1 ) );
    return 1;
}


static int gc_lua( lua_State *L )
{
    usher_dealloc( ((lusher_t*)lua_touserdata( L, 1 ))->usher );
    return 0;
}


static void udata_dealloc_cb( void *data )
{
    lusher_udata_t *udata = (lusher_udata_t*)data;
    
    if( udata->data )
    {
        // release reference
        switch( udata->type ){
            case LUA_TSTRING:
            case LUA_TTABLE:
            case LUA_TFUNCTION:
            case LUA_TUSERDATA:
            case LUA_TTHREAD:
            case LUA_TLIGHTUSERDATA:
                lstate_unref( udata->L, (int)udata->data );
            break;
        }
    }
    pdealloc( data );
}


static int alloc_lua( lua_State *L )
{
    lusher_t *u = NULL;
    const char *delim = NULL;
    
    // check argument
    if( lua_gettop( L ) > 0 )
    {
        size_t len = 0;
        
        delim = luaL_checklstring( L, 1, &len );
        if( len != 3 ){
            return luaL_argerror( L, 1, "delimiter string length must be equal to 3" );
        }
    }
    
    // allocate
    if( ( u = lua_newuserdata( L, sizeof( lusher_t ) ) ) &&
        ( u->usher = usher_alloc( delim, udata_dealloc_cb ) ) ){
        luaL_getmetatable( L, MODULE_MT );
        lua_setmetatable( L, -2 );
        return 1;
    }
    
    // got error
    lua_pushnil( L );
    lua_pushstring( L, strerror( errno ) );
    
    return 2;
}


LUALIB_API int luaopen_usher( lua_State *L )
{
    struct luaL_Reg mmethod[] = {
        { "__gc", gc_lua },
        { "__tostring", tostring_lua },
        { NULL, NULL }
    };
    struct luaL_Reg method[] = {
        { "set", set_lua },
        { "get", get_lua },
        { "exec", exec_lua },
        { "dump", dump_lua },
        { NULL, NULL }
    };
    int i;
    
    // create table __metatable
    luaL_newmetatable( L, MODULE_MT );
    // metamethods
    i = 0;
    while( mmethod[i].name ){
        lstate_fn2tbl( L, mmethod[i].name, mmethod[i].func );
        i++;
    }
    // methods
    lua_pushstring( L, "__index" );
    lua_newtable( L );
    i = 0;
    while( method[i].name ){
        lstate_fn2tbl( L, method[i].name, method[i].func );
        i++;
    }
    lua_rawset( L, -3 );
    lua_pop( L, 1 );
    
    // create table
    lua_newtable( L );
    // add new function
    lstate_fn2tbl( L, "new", alloc_lua );
    // add status code
    lstate_int2tbl( L, "OK", USHER_OK );
    lstate_int2tbl( L, "EINVAL", USHER_EINVAL );
    lstate_int2tbl( L, "ENOMEM", USHER_ENOMEM );
    lstate_int2tbl( L, "EFORMAT", USHER_EFORMAT );
    lstate_int2tbl( L, "ESPLIT", USHER_ESPLIT );
    lstate_int2tbl( L, "EALREADY", USHER_EALREADY );
    lstate_int2tbl( L, "ENOENT", USHER_ENOENT );

    return 1;
}

