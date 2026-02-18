"use client";

import { Bell, Search } from 'lucide-react';
import Image from 'next/image';

export default function Header() {
    return (
        <header className="flex justify-between items-center mb-8">
            <div>
                <h2 className="text-2xl font-bold text-[#303972]">Dashboard</h2>
                <p className="text-gray-500 text-sm">Welcome to your dashboard</p>
            </div>

            <div className="flex items-center gap-6">
                {/* Search */}
                <div className="relative hidden md:block">
                    <div className="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
                        <Search className="w-4 h-4 text-gray-400" />
                    </div>
                    <input
                        type="text"
                        className="block w-full p-2.5 ps-10 text-sm text-gray-900 border border-transparent rounded-full bg-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400 shadow-sm min-w-[300px]"
                        placeholder="Search here..."
                    />
                </div>

                {/* Notifications */}
                <button className="relative p-2 text-gray-400 hover:text-gray-500 transition-colors bg-white rounded-full shadow-sm">
                    <Bell className="w-6 h-6" />
                    <span className="absolute top-2 right-2.5 block h-2 w-2 rounded-full ring-2 ring-white bg-red-500 transform translate-x-1/2 -translate-y-1/2"></span>
                </button>

                {/* User Profile */}
                <div className="flex items-center gap-3 ps-3 border-l border-gray-200">
                    <div className="text-end hidden sm:block">
                        <div className="text-sm font-bold text-[#303972]">Nabila A.</div>
                        <div className="text-xs text-gray-500">Admin</div>
                    </div>
                    <div className="w-10 h-10 rounded-full bg-slate-200 overflow-hidden border-2 border-white shadow-sm relative">
                        <Image
                            src="https://ui-avatars.com/api/?name=Nabila+A&background=random"
                            alt="User"
                            fill
                            className="object-cover"
                        />
                    </div>
                </div>
            </div>
        </header>
    );
}
