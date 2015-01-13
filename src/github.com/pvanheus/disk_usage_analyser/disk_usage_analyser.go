package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"os"
	"path/filepath"
	"syscall"
	//	"labix.org/v2/mgo/bson"
	//	"path"
	"flag"
	"strings"
	"sync"
)

type TreeInfo struct {
	Root    string
	Entries int64
	Size    int64
	Uid     uint32
}
type WalkFunc func(path string, info os.FileInfo, err error) error

var work_units int = 1
var work_unit_mutex sync.Mutex

func walk(path string, info os.FileInfo, walkFn WalkFunc) error {
	err := walkFn(path, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}

	if !info.IsDir() {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return err
	}

	for _, name := range names {
		filename := filepath.Join(path, name)
		info, err := os.Lstat(filename)
		if err != nil {
			return err
		} else {
			err = walk(filename, info, walkFn)
			if err != nil {
				if err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}

func walkDir(root string, walkFn WalkFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		return walkFn(root, nil, err)
	}
	return walk(root, info, walkFn)
}

var bigSize int64 = 1024000 * 1000 * 10 // 10 GB
func tree_size(path_channel chan string, result_channel chan *TreeInfo, excludedDirs map[string]bool) {
	for root := range path_channel {
		var path_info syscall.Stat_t
		syscall.Stat(root, &path_info)
		current_uid := path_info.Uid
		current_tree := new(TreeInfo)
		current_tree.Root = root
		current_tree.Uid = current_uid
		var walker WalkFunc = func(current_path string, fileInfo os.FileInfo, err error) error {
			if err == nil {
				//fmt.Println(current_path)
				syscall.Stat(current_path, &path_info)
				current_tree.Entries++
				if !fileInfo.IsDir() {
					current_tree.Size += fileInfo.Size()
				} else {
					if excludedDirs[current_path] == true {
						return filepath.SkipDir
					} else if path_info.Uid != current_uid {
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
		walkDir(root, walker)
		result_channel <- current_tree
	}
}

func main() {
	var root_path string
	var clearDb = flag.Bool("C", false, "Clear all entries in the DB before computing disk usage")
	var aggregateUser = flag.Bool("U", false, "Only report total disk usage per user")
	var excludeDirsString = flag.String("X", "", "Comma separated list of directories to exclude")
	user_totals := make(map[uint32]TreeInfo)
	flag.Parse()
	if flag.NArg() == 1 {
		root_path = flag.Arg(0)
	} else {
		//root_path = "/home/pvh/Videos"
		panic("require command line argument")
	}
	excludedDirs := make(map[string]bool)
	if *excludeDirsString != "" {
		for _, dirname := range strings.Split(*excludeDirsString, ",") {
			excludedDirs[dirname] = true
		}
	}
	//	fmt.Println("Hello world!")
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	c := session.DB("disk_usage").C("trees")
	if *clearDb == true {
		fmt.Println("deleting all data in disk_usage DB")
		c.RemoveAll(nil)
	}
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
		go tree_size(path_channel, result_channel, excludedDirs)
	}

	path_channel <- root_path
	fmt.Println("Root:", root_path)
	for work_units > 0 {
		select {
		case tree, result_ok := <-result_channel:
			if result_ok {
				//treeSizes[tree.Root] = tree
				if *aggregateUser == true {
					total_tree := user_totals[tree.Uid]
					total_tree.Root = root_path
					total_tree.Uid = tree.Uid
					total_tree.Entries += tree.Entries
					total_tree.Size += tree.Size
					user_totals[tree.Uid] = total_tree
				} else {
					err = c.Insert(tree)
					if err != nil {
						panic(err)
					}
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

	if *aggregateUser == true {
		for _, tree := range user_totals {
			fmt.Println(tree.Root, tree.Uid, tree.Size, tree.Entries)
			c.Insert(tree)
		}
	} else {
		trees := []TreeInfo{}
		err = c.Find(nil).All(&trees)
		if err != nil {
			panic(err)
		}
		for _, tree := range trees {
			fmt.Println(tree.Root, tree.Size, tree.Entries)
		}
	}
	//	for root, tree := range treeSizes {
	//		fmt.Println(root, tree.Size, tree.Entries)
	//	}
}
