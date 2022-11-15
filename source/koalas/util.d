module koalas.util;

import std.meta: AliasSeq, staticIndexOf;
import std.traits : FieldNameTuple, Fields;
import std.typecons : Tuple;
import std.conv      : to ;
import std.algorithm : startsWith ;
import std.array     : split ;

/// Zip two AliasSeq's together
/// e.g.
/// Zip!(AliasSeq!(int, long), AliasSeq!("a", "b"))
/// returns
/// AliasSeq!(int, "a", long, "b")
template Zip(args...) {
    static if(args.length % 2 != 0)
        pragma(msg, "Not even number of fields to zip");
    else static if(args.length == 2) {
        alias Zip = AliasSeq!(args[0], args[1]);
    } else {
        alias head = args[1 .. args.length / 2];
        alias tail = args[(args.length / 2) + 1 .. $];
        alias Zip = AliasSeq!(args[0], args[args.length / 2], Zip!(head, tail));
    }
}


/// Subset fields from AliasSeq used to create a tuple
/// e.g.
/// Subset!(["a"], AliasSeq!(int, "a", long, "b"))
/// returns
/// AliasSeq!(int, "a")
template Subset(string[] fields, Args...) {
    static if(fields.length == 0){
        alias Subset = AliasSeq!();
    } else {
        enum fieldId = staticIndexOf!(fields[0], Args);
        static if(fieldId == -1)
            static assert("Subsetted field doesn't exist \""~fields[0]~"!");
        else
            alias Subset = AliasSeq!(Args[fieldId - 1], Args[fieldId], Subset!(fields[1..$], Args));
    }
}

/// Subset fields from AliasSeq using another template
/// used in groupby
/// alias Idx = Tuple!(AliasSeq!(int, "a"));
/// e.g.
/// SubsetByTemplate!(isNumeric, Idx, AliasSeq!(int, "a", long, "b", string, "c"))
/// returns
/// AliasSeq!(int, "a", long, "b")
template SubsetByTemplate(alias Template, IT, Args...) {
    static if(Args.length % 2 != 0)
        pragma(msg, "Not even number of fields to subset");
    else static if(Args.length == 0){
        alias SubsetByTemplate = AliasSeq!();
    } else {
        static if(Template!(Args[0]) && staticIndexOf!(Args[1], IT.fieldNames) == -1)
            alias SubsetByTemplate = AliasSeq!(Args[0], Args[1], SubsetByTemplate!(Template, IT, Args[2..$]));
        else
            alias SubsetByTemplate = AliasSeq!(SubsetByTemplate!(Template, IT, Args[2..$]));
    }
}

/// Subset tuple used for subsetting dataframe
auto subsetTuple(T, string[] fields, Args...)(T source) {
    Tuple!(Subset!(fields, Args)) ret;
    static foreach (f; fields)
    {
        mixin("ret."~f~" = source."~f~";");
    }
    return ret;
}

/// libmir doesn't have a multisort so here we create it
/// for any set of tuples
auto mirMultiSort(T1, T2)(T2 a, T2 b) {
    
    static foreach (name; T1.fieldNames)
    {
        mixin("if(a."~name~" != b."~name~") { return a."~name~" < b."~name~";}");
    }
    return 0;
}