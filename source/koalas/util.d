module koalas.util;

import std.meta: AliasSeq;
import std.conv      : to ;
import std.algorithm : startsWith ;
import std.array     : split ;


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

/// Generate struct based on another struct by extract properties listed in fields
template GenSubset(string name, string[] fields, OT){
    enum GenSubset = GenSubsetString!(OT,name,fields);
}

/// Generate multisort comparisions based on type
string[] GenMultiSortComps(T)(){
    alias memberNames = AliasSeq!(T.tupleof);
    string[] ret;
    static foreach (member; memberNames)
    {
        ret ~="a."~member.stringof~" < b."~member.stringof;
    }
    return ret;
}

/// Generate multisort based on type
string GenMultiSort(T)(string var){
    if(GenMultiSortComps!T.length == 0) return "{ }";
    string ret = var~".multiSort!(";
    foreach(s;GenMultiSortComps!T){
        ret~="\""~s~"\",";
    }
    ret = ret[0..$-1]~");";
    return ret;
}

/// TODO: figure out if hashing struct fields
/// helps with executable size
/// Also figure out how to hash at compile time
string GenIndexName(string name, string[] fields)(){
    string ret = name ~ "_Idx";
    static foreach(field;fields){
        ret~=field;
    }
    return ret;
}

/// Generate struct from another, adding new members via additional
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

private struct S_B { int x; int z;} 
                                                     
unittest 
{     
    import std.stdio: writeln;
     
    S_B s_ab;
    s_ab.x = 10;

    mixin(GenSubset!("sub",["z"],S_B));
    pragma(msg, __traits(allMembers, sub));
    import std.algorithm: multiSort;
    sub[] arr = new sub[10];
    arr[1].z = 2;
    assert(GenMultiSortComps!sub == ["a.z < b.z"]);
    mixin(GenMultiSort!sub("arr"));
}