// AES 加解密:AES/GCM/NoPadding 实现,用于实例密码的落库与读取
package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	// ivLength GCM 模式要求的 IV 长度
	ivLength = 12
	// storedPrefix 密文存储格式前缀
	storedPrefix = "AES$"
	// storedSeparator 存储格式分隔符,Base64 字符集中不含该字符,拆分安全
	storedSeparator = "$"
	// keyLength AES-256 密钥长度
	keyLength = 32
)

// NewKey 解析 Base64 编码的密钥并校验长度
func NewKey(base64Key string) ([]byte, error) {

	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(base64Key))
	if err != nil {
		return nil, fmt.Errorf("AES 密钥不是合法的 Base64 字符串: %w", err)
	}
	if len(key) != keyLength {
		return nil, fmt.Errorf("AES 密钥解码后须为 32 字节, 实际 %d 字节", len(key))
	}
	return key, nil
}

// Encrypt 加密为落库存储格式 AES$IV$密文
// 字符串为空时原样返回, 不加密
func Encrypt(plain string, key []byte) (string, error) {

	if plain == "" {
		return plain, nil
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	iv := make([]byte, ivLength)
	if _, err := rand.Read(iv); err != nil {
		return "", fmt.Errorf("生成 IV 失败: %w", err)
	}
	sealed := gcm.Seal(nil, iv, []byte(plain), nil)
	return storedPrefix + base64.StdEncoding.EncodeToString(iv) + storedSeparator + base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt 解密落库存储格式,不带 AES$ 前缀视为存量明文原样返回
func Decrypt(stored string, key []byte) (string, error) {

	if stored == "" {
		return stored, nil
	}
	if !strings.HasPrefix(stored, storedPrefix) {
		return stored, nil
	}
	parts := strings.Split(stored, storedSeparator)
	if len(parts) != 3 {
		return "", fmt.Errorf("AES 存储格式密文结构无效, 段数 %d", len(parts))
	}
	iv, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("AES IV 不是合法 Base64: %w", err)
	}
	if len(iv) != ivLength {
		return "", fmt.Errorf("AES GCM IV 长度必须为 12 字节, 实际 %d 字节", len(iv))
	}
	data, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("AES 密文不是合法 Base64: %w", err)
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, iv, data, nil)
	if err != nil {
		return "", fmt.Errorf("AES 解密失败, 密文或密钥不匹配: %w", err)
	}
	return string(plain), nil
}

// newGCM 构造 AES-GCM 加解密器,认证标签长度默认 128 位
func newGCM(key []byte) (cipher.AEAD, error) {

	if len(key) != keyLength {
		return nil, errors.New("AES 密钥长度必须为 32 字节")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("初始化 AES 失败: %w", err)
	}
	return cipher.NewGCM(block)
}
