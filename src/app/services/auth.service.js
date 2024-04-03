import { API_BASE_URL } from "../utils/url";
import { BaseApi } from "./base.service";

class AuthApi extends BaseApi {
  authConfig = {
    signInUrl: "auth/sign-in",
    signUpUrl: "auth/sign-up",
    sendOtpUrl: "auth/otp",
    signOutUrl: "auth/logout",
    verifyOtpUrl: "auth/verify-otp",
    verifyTokenUrl: "auth/verify-token",
  };

  constructor() {
    super(API_BASE_URL);
  }

  static getToken() {
    return localStorage.getItem("token");
  }

  setToken(token) {
    localStorage.setItem("token", token);
    this.axiosInstance.defaults.headers.common["auth-token"] = token;
  }

  setSession(token) {
    this.axiosInstance.defaults.headers.common["auth-token"] = token;
  }

  removeToken() {
    localStorage.removeItem("token");
    delete this.axiosInstance.defaults.headers.common["auth-token"];
  }

  sendOtp(data) {
    return this.axiosInstance.post(this.authConfig.sendOtpUrl, data);
  }

  verifyOtp(data, config) {
    return this.axiosInstance.post(this.authConfig.verifyOtpUrl, data, config);
  }

  verifyToken() {
    return this.axiosInstance.get(this.authConfig.verifyTokenUrl);
  }

  signUp(credentails) {
    return this.axiosInstance.post(this.authConfig.signUpUrl, credentails);
  }

  signIn(credentails) {
    return this.axiosInstance.post(this.authConfig.signInUrl, credentails);
  }

  signOut() {
    return this.axiosInstance.post(this.authConfig.signOutUrl);
  }
}

export const authApi = new AuthApi();