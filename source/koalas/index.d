module koalas.index;

import koalas.dataframe;
import std.meta;
import std.range;
import mir.ndslice;
import std.algorithm.sorting : multiSort;
import std.array : array;

struct Index(Args...) {
    Dataframe!(Args) * df;
    ulong[] idx;
    alias idx this;

    this(Dataframe!(Args) * df) {
        this.df = df;
        this.idx = iota!(size_t, 1)(df.length).ndarray;
    }

    bool less(string col)(ulong a, ulong b) {
        mixin("return cast(bool)(df.records[a]."~col~" < df.records[b]."~col~");");
    }
    
    void sort(indices...)() {
        mixin(generateSort([indices]));
    }   
    
}

string generateSort(string[] cols) {
    string ret = "idx = multiSort!(";
    foreach(i;cols){
        ret ~= "(a, b) => df.records[a]."~i~" < df.records[b]."~i~",";
    }
    return ret[0..$-1] ~")(idx).array;";
}