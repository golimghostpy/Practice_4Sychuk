#ifndef STRUCTURES_H_INCLUDED
#define STRUCTURES_H_INCLUDED

#include "libs.h"

enum class SQLRequest{
    SELECT,
    INSERT,
    DELETE,
    END,
    UNKNOWN
};

template <typename T>
struct Node{
    T data;
    Node* next;
    Node(T val) : data(val), next(nullptr) {}
};

struct IntList{ // список чисел
    Node<int>* first;
    Node<int>* last;
    int listSize;

    IntList() : first(nullptr), last(nullptr), listSize(0) {}

    bool is_empty();
    void push_back(int);
    Node<int>* find(int);
    void print(string);
    void clear();
};

struct BoolList{ // список булевых элементов
    Node<bool>* first;
    Node<bool>* last;

    BoolList() : first(nullptr), last(nullptr) {}

    bool is_empty();
    void push_back(bool);
    void clear();
};

struct StringList{ // список строк
    int listSize;
    Node<string>* first;
    Node<string>* last;

    StringList() : listSize(0), first(nullptr), last(nullptr) {}

    bool is_empty();
    void push_back(string);
    string print(string);
    Node<string>* find(int);
    Node<string>* word_find(const string&);
    string join(const char);
    int index_word(const string&);
    void clear();
};

struct MatrixNode{
    string data;
    MatrixNode* nextRow;
    MatrixNode* nextCol;

    MatrixNode(string val) : data(val), nextRow(nullptr), nextCol(nullptr) {}
};

struct StringMatrix{ // матрица, заполняемая по стобцам
    MatrixNode* firstCol;
    MatrixNode* lastCol;

    StringMatrix() : firstCol(nullptr), lastCol(nullptr){}

    bool is_empty();
    void push_right(string);
    void push_down(string, int);
    string print();
    void clear();
    MatrixNode* get_at(int, int);
};

struct Array {
    char* data;
    int size;
    int capacity;

    Array(int initialCapacity = 10) : size(0), capacity(initialCapacity){
        data = new char[capacity];
    }

    ~Array() {
        delete[] data;
    }

    char* get();
};

#endif // STRUCTURES_H_INCLUDED
