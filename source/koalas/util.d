module koalas.util;

import std.traits    : isCallable, PIT=ParameterIdentifierTuple, PTT=ParameterTypeTuple;
import std.meta: AliasSeq;
import std.conv      : to ;
import std.algorithm : startsWith ;
import std.array     : split ;

template GenStructs(uint count, U...)
{
    static if (U.length == 0)
    {
        enum GenStructs = "\n";    
    }
    else
    {
        enum GenStructs = "\n private " ~ (U[0]).stringof ~ " _s" ~ to!string(count) ~ ";" ~ 
                          GenStructs!(1+count, U[1..$]);
    }
}

template Filter(U...)
{
    static if (U.length == 0)
    {
        enum Filter = "";
    }
    else
    {
        static if ( (U[0]).startsWith("__") || (U[0]).startsWith("op") )
        {
            enum Filter = Filter!(U[1..$]); //skip U[0]
        }
        else
        {
            enum Filter = U[0] ~ "," ~ Filter!(U[1..$]);
        }
    }
}

template StringOf(TS...)
{
    static if(TS.length == 0)
    {
        enum StringOf = "";
    }
    else static if(TS.length == 1)
    {
        enum StringOf = (TS[0]).stringof;
    }
    else
    {
        enum StringOf = (TS[0]).stringof ~ "," ~ StringOf!(TS[1..$]) ;
    }
}

template ArgStringOf(TS...)
{
    static if(TS.length == 0)
    {
        enum ArgStringOf = "";
    }
    else static if(TS.length == 1)
    {
        enum ArgStringOf = TS[0];
    }
    else
    {
        enum ArgStringOf = TS[0] ~ "," ~ ArgStringOf!(TS[1..$]);
    }
}

string combine(string[] types, string[] members)
{
    assert(types.length == members.length);
    
    string combined = "";
    
    for(int i=0; i < (types.length) ; ++i)
    {
        combined ~= types[i] ~ " " ~ members[i] ~ ", ";
    }
    
    if(combined != "") combined = combined[0..$-2] ; //trim end ", "
    
    return combined;
}

template GenFunction(string M, string N, alias SN)
{
    enum GenFunction = `
                        auto ref ` ~ N ~ `(` ~ 
                        combine( StringOf!(PTT!(SN)).split(","),
                                 ArgStringOf!(PIT!(SN)).split(",") )~ `)
                        { return ` ~ M ~ `(` ~ ArgStringOf!(PIT!(SN)) ~ `); }`;
}

string genProperty(string mem, string name, string s_name)
{
    return `static if ( !__traits(compiles, ` ~ name ~ `) )
            {    static if (isCallable!(` ~ s_name ~ `.` ~ name ~ `))
                {
                    mixin (GenFunction!( "`~ mem ~`", "`~ name ~`", `~ s_name ~`.`~ name ~`));
                }
                else
                {
                    @property auto ref ` ~ name ~ `() { return ` ~ mem ~ `; };
                    @property ` ~ name ~ `(typeof(` ~ s_name ~ `.` ~ name ~ `) _` ~ name ~ `)
                    { ` ~ mem ~ ` = _` ~ name ~ `; }
                }
            }
            else
            {
            }
            `;
}

string genAlias(uint id, string[] members, string s_name)
{
    string output;

    foreach(m ; members)
    {    
        output ~= genProperty("this._s" ~ to!string(id) ~ "." ~ m, m, s_name);
    }
    
    return output;
}

template GenAliases(uint count, U...)
{
    static if (U.length == 0)
    {
        enum GenAliases = "";    
    }
    else
    {
        enum GenAliases = genAlias(count, 
                                   Filter!(__traits(allMembers, U[0]))[0..$-1].split(","),
                                   U[0].stringof) ~ GenAliases!(1+count, U[1..$]);
    }
}
                                                     
template Gen(string name, U...)
{
    static assert(U.length != 0);
    
    enum Gen = `struct ` ~ name ~ ` { ` ~  GenStructs!(0, U) ~ 
                                           GenAliases!(0, U) ~ ` }`;
}

string GenSubsetString(OT,string name, string[] fields)(){
    static foreach (field; fields)
    {
        static assert(__traits(hasMember, OT, field));
    }
    string ret = "struct " ~ name ~ " { ";
    static foreach(field;fields){
        mixin("ret ~= typeof(OT."~field~").stringof ~ \" "~field~";\";");
    }
    ret ~= "}";
    return ret;
}

template GenSubset(string name, string[] fields, OT){
    enum GenSubset = GenSubsetString!(OT,name,fields);
}

string[] GenMultiSortComps(T)(){
    alias memberNames = AliasSeq!(T.tupleof);
    string[] ret;
    static foreach (member; memberNames)
    {
        ret ~="a."~member.stringof~" < b."~member.stringof;
    }
    return ret;
}

string GenMultiSort(T)(string var){
    string ret = var~".multiSort!(";
    foreach(s;GenMultiSortComps!T){
        ret~="\""~s~"\",";
    }
    ret = ret[0..$-1]~");";
    return ret;
}

string GenIndexName(string name, string[] fields)(){
    string ret = name ~ "_Idx";
    static foreach(field;fields){
        ret~=field;
    }
    return ret;
}

string GenAddStructString(OT,string name, string additional)(){
    alias memberTypes = AliasSeq!(typeof(OT.tupleof));
    alias memberNames = AliasSeq!(OT.tupleof);
    string ret = "struct " ~ name ~ " { ";
    static foreach(i,member;memberNames){
        ret ~= memberTypes[i].stringof ~ " " ~member.stringof~";";
    }
    ret ~= additional;
    ret ~= "}";
    return ret;
}

struct S_A { int x; int y; void func() { x = 2*x; } ; void funcA() { } ; }
struct S_B { int x; int z; void func() { x = 3*x; } ; void funcB() { } ; } 
                                                     
unittest 
{     
    import std.stdio: writeln;
    
    mixin (Gen!("S_AB", S_A, S_B));
    pragma(msg, __traits(allMembers, S_AB));
     
    S_AB s_ab;
    s_ab.x = 10;
    s_ab.func();
    assert(s_ab.x == 20);

    mixin(GenSubset!("sub",["z"],S_AB));
    pragma(msg, __traits(allMembers, sub));
    import std.algorithm: multiSort;
    sub[] arr = new sub[10];
    arr[1].z = 2;
    writeln(GenMultiSortComps!sub);
    mixin(GenMultiSort!sub("arr"));
    writeln(arr);
}