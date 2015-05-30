package kademlia

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"sss"
	"strings"
	"time"
)

type VanashingDataObject struct {
	AccessKey  int64
	Ciphertext []byte
	NumberKeys byte
	Threshold  byte
}

func GenerateRandomCryptoKey() (ret []byte) {
	for i := 0; i < 32; i++ {
		ret = append(ret, uint8(mathrand.Intn(256)))
	}
	return
}

func GenerateRandomAccessKey() (accessKey int64) {
	r := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	accessKey = r.Int63()
	return
}

func CalculateSharedKeyLocations(accessKey int64, count int64) (ids []ID) {
	r := mathrand.New(mathrand.NewSource(accessKey))
	ids = make([]ID, count)
	for i := int64(0); i < count; i++ {
		for j := 0; j < IDBytes; j++ {
			ids[i][j] = uint8(r.Intn(256))
		}
	}
	return
}

func encrypt(key []byte, text []byte) (ciphertext []byte) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	ciphertext = make([]byte, aes.BlockSize+len(text))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], text)
	return
}

func decrypt(key []byte, ciphertext []byte) (text []byte) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	if len(ciphertext) < aes.BlockSize {
		panic("ciphertext is not long enough")
	}
	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(ciphertext, ciphertext)
	return ciphertext
}

func VanishData(kadem Kademlia, data []byte, numberKeys byte, threshold byte) (vdo VanashingDataObject) {
	K := GenerateRandomCryptoKey()
	C := encrypt(K, data)
	shares, err := sss.Split(numberKeys, threshold, K)
	if err != nil {
		panic(err)
	}
	L := GenerateRandomAccessKey()
	ids := CalculateSharedKeyLocations(L, int64(numberKeys))
	//store N shares to the closest ids
	for x := 0; x < int(numberKeys); x++ {
		key := byte(x + 1)
		//append the key at last
		all := append(shares[key], key)
		(&kadem).DoIterativeStore(ids[x], all)
	}
	vdo = VanashingDataObject{L, C, numberKeys, threshold}
	return vdo
}

func UnvanishData(kadem Kademlia, vdo VanashingDataObject) (data []byte) {
	L := vdo.AccessKey
	C := vdo.Ciphertext
	N := vdo.NumberKeys
	T := vdo.Threshold
	ids := CalculateSharedKeyLocations(L, int64(N))
	// number of shares found
	count := 0
	shares := make(map[byte][]byte, T)
	//find at least T shares to restore the decrypt Key
	for x := 0; x < int(N); x++ {
		//str is "ERR" if value is not found, else value is after "found value: " in str
		str := (&kadem).DoIterativeFindValue(ids[x])
		if str == "ERR" {
			continue
		}
		//get the value out of str
		index := strings.LastIndex(str, "found value: ")
		index = index + 13
		str = str[index:]
		all := []byte(str)
		k := all[len(all)-1]
		v := all[0 : len(all)-1]
		//store the key-value pair in shares
		shares[k] = v
		//increment the count
		count++
		//stop when at least T shares have been found
		if count >= int(T) {
			break
		}
	}
	//obtain the original K
	K := sss.Combine(shares)
	data = decrypt(K, C)

	return data
}
