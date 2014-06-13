package main

import (
	"fmt"
	"path/filepath"
	"os"
	"syscall"
	"labix.org/v2/mgo"
//	"labix.org/v2/mgo/bson"
//	"path"
	"sync"
)

type TreeInfo struct {
	Root string
	Entries int64
	Size int64
	Uid uint32
}
var work_units int = 1
var work_unit_mutex sync.Mutex

func tree_size(path_channel chan string, result_channel chan *TreeInfo) {
	for root := range path_channel {
		var path_info syscall.Stat_t
		syscall.Stat(root, &path_info)
		current_uid := path_info.Uid
		current_tree := new(TreeInfo)
		current_tree.Root = root
		current_tree.Uid = current_uid
		var walker filepath.WalkFunc = func(current_path string, fileInfo os.FileInfo, err error) error {
			if err == nil {
				//fmt.Println(current_path)
				syscall.Stat(current_path, &path_info)
				current_tree.Entries++
				if ! fileInfo.IsDir() {
					current_tree.Size += fileInfo.Size()
				} else {
					if path_info.Uid != current_uid {
						path_channel <- current_path
						work_unit_mutex.Lock()
						work_units += 1
						work_unit_mutex.Unlock()
						//fmt.Println("skip", current_path)
						return filepath.SkipDir
					}
				}
			}
			return nil
		}
		filepath.Walk(root, walker)
		result_channel <- current_tree
	}
}

func main() {
	var root_path string
	if len(os.Args) > 1 {
		root_path = os.Args[1]
	} else {
		//root_path = "/home/pvh/Videos"
		panic("require command line argument")
	}
//	fmt.Println("Hello world!")
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	c := session.DB("disk_usage").C("trees")
	c.RemoveAll(nil)
//	err = c.Insert(&TreeInfo{Root: "/var/tmp", Uid: 0},
//		&TreeInfo{Root: "/home/pvh", Uid: 1025})
//	if err != nil {
//		panic(err)
//	}
//	result := TreeInfo{}
//	err = c.Find(bson.M{"root": "/home/pvh"}).One(&result)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(result)
	//dirSizes := make(map[string]int64)
	//treeSizes := make(map[string]*TreeInfo)
	//dirSize := int64(0)
	//container := "/home/pvh"
	num_workers := 4
	path_channel := make(chan string, 10000)
	result_channel := make(chan *TreeInfo, 100)
	for i := 0; i < num_workers; i++ {
		go tree_size(path_channel, result_channel)
	}

	path_channel <- root_path
	fmt.Println("Root:", root_path)
	for work_units > 0 {
		select {
		case tree, result_ok := <-result_channel:
			if result_ok {
				//treeSizes[tree.Root] = tree
				err = c.Insert(tree)
				if err != nil {
					panic(err)
				}
				work_unit_mutex.Lock()
				work_units--
				work_unit_mutex.Unlock()
				fmt.Println("Result stored for:", tree.Root)
			}
		}
	}
	close(result_channel)
	close(path_channel)

	trees := []TreeInfo{}
	err = c.Find(nil).All(&trees)
	if err != nil {
		panic(err)
	}
	for _, tree := range trees {
		fmt.Println(tree.Root, tree.Size, tree.Entries)
	}
//	for root, tree := range treeSizes {
//		fmt.Println(root, tree.Size, tree.Entries)
//	}
}
